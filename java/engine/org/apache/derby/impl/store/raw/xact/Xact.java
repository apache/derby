/*

   Derby - Class org.apache.derby.impl.store.raw.xact.Xact

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.Limit;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Compensation;
import org.apache.derby.iapi.store.raw.GlobalTransactionId;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.data.DataFactory;
import org.apache.derby.iapi.store.raw.data.RawContainerHandle;

import org.apache.derby.iapi.store.raw.xact.RawTransaction; 
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.log.Logger;

import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.error.ExceptionSeverity;

import org.apache.derby.iapi.services.property.PersistentSet;

import org.apache.derby.catalog.UUID;

import java.util.Stack;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.Dictionary;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream;
import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.Property;

import org.apache.derby.impl.store.raw.log.LogToFile;

import org.apache.derby.iapi.services.io.LimitObjectInput;

import org.apache.derby.iapi.services.context.ContextService;

/**

  A transaction has five states
  <OL>
  <LI> CLOSED - cannot be used
  <LI> IDLE - no reads have been performed by the transaction.
  <LI> ACTIVE - at least one read has been attempted by the transaction
  <LI> UPDATE - at least one update has been attempted by the transaction
  <LI> PREPARED - the transaction is ready to commit (FUTURE).
  </OL>
  <BR>Transaction identifiers are re-used for transactions that do not enter the
  UPDATE state during their lifetime.

	@see Transaction

*/
public class Xact extends RawTransaction implements Limit  {

	/*
	** Static Fields
	*/

	protected static final int	CLOSED		    = 0;
	protected static final int	IDLE		    = 1;
	protected static final int	ACTIVE		    = 2;
	protected static final int	UPDATE		    = 3;
	protected static final int	PREPARED	    = 4;


	/*
	**	Transaction status stored in the beginXact and endXact
	*/

	public static final int END_ABORTED             = 0x00000001;
	public static final int END_PREPARED            = 0x00000002;
	public static final int END_COMMITTED           = 0x00000004;

	public static final int RECOVERY_ROLLBACK_FIRST = 0x00000010;
	public static final int INTERNAL_TRANSACTION    = 0x00000020;
	public static final int NESTED_TOP_TRANSACTION  = 0x00000040;


	/**
	  private static - 

	  make sure these bits don't overwrite bits in Transaction.commit commitflag
	*/
	private static final int COMMIT_SYNC            = 0x00010000;
	private static final int COMMIT_NO_SYNC         = 0x00020000;
	private static final int COMMIT_PREPARE         = 0x00040000;


	/*
	** Fields
	*/

	//
	// set during recovery if this is the recovery transaction.  
	//
	private int savedEndStatus;	

	//
	// if this transaction object was committed without syncing, then in a
	// subsequent commit with sync, the log must be flushed even if the last
	// transaction is read only
	private boolean needSync;

	//
	// When the xact is first created, it is in an IDLE state.  Since the
	// transaction table needs an XactId, one will be made for it.  When this
	// transaction commits, it goes back to the IDLE state.  If we then create
	// a new XactId for it, we will waste one XactId per transaction object
	// because it must first go thru the IDLE state before it gets closed.
	// Therefore, the first XactId is assigned in the constructor and
	// subsequent XactId is assigned in the setActiveState.  However, the first
	// time it goes into setActiveState, we don't want it to create a new
	// XactId when the one that was assigned to it in the constructore is good
	// enough, so we use this justCreate field to indicate to setActiveState
	// whether it needs to make a new XactId (for the next transaction) for
	// not. 
	private boolean justCreated = true;


	protected	XactContext		xc;	// my context - set by XactContext

	// these fields remain fixed for the lifetime of this object
	protected final XactFactory		xactFactory;
	protected final DataFactory		dataFactory;
	protected final LogFactory		logFactory;
	protected final Object   		compatibilitySpace;

	// these fields remain fixedfor the lifetime
	private LockingPolicy defaultLocking;

	// Global id, unique among all rawstores and all eternity
	private GlobalTransactionId	myGlobalId;

	// id that is valid locally in this raw store.
	private volatile TransactionId	myId;

	protected Logger		logger;		// the object we use to access the log.


	protected volatile int	state; 		// we access this without synchronization sometimes
	private Integer		inComplete = null;	// set between preComplete() and postComplete()

	private boolean		seenUpdates; // true if this session has written a log
									 // record to disk.  Note this is per
									 // session and not per transaction, namely
									 // during recovery, a transaction may have
									 // updates but it may not have any updates
									 // during recovery.  In that case,
									 // seenUpdates is false even though state
									 // is UPDATE. 
									 // This value is used to decide whether
									 // the log needs to get flushed at commit
									 // time. 

	private boolean	inPostCommitProcessing; // true if we are processing post
									  // commit work in the same context the
									  // work was queued.  This is used to stop
									  // recursion only.  We don't want a post
									  // commit task to queue other post commit
									  // task, ad infinitum.  PostCommitWork
									  // requested while processing
									  // postCommitWork will be processed 
									  // by the daemon, which may itself
									  // recurse once.

	private LogInstant		logStart; // If this is a read only transaction (has
									  // never written a log record to disk),
									  // then null.  Otherwise, set to the log
									  // instant of the first log record.

	private LogInstant		logLast;  // the last log record written by this
									  // transaction 

	private Stack			savePoints;	// stack of SavePoint objects.

	protected List   		postCommitWorks; // a list of post commit work
	protected List		    postTerminationWorks; // work to be done after
												  // transaction terminates,
												  // commit or abort
	private boolean			recoveryTransaction;  // this transaction is being
												  // used by recovery

	DynamicByteArrayOutputStream logBuffer;

	private boolean postCompleteMode;	// perform most preComplete work in postComplete

	// Use this flag to catch the case where a global transaction was closed.
	// Normally a closed transaction should not be aborted again, but we need
	// to allow abort of a closed global transaction for error handling.  Use
	// this flag to make sure people are not abusing this loop hole.
	// RESOLVE: sku to remove before GA
	private boolean sanityCheck_xaclosed;

    // Indicates the name of the transaction, and if it is set, it is displayed
    // by the transactiontable VTI
    private String transName;

    // The transaction is only allowed read operations, no log writes.
    private boolean         readOnly;



	/*
	** Constructor
	*/

	protected Xact(
    XactFactory xactFactory, 
    LogFactory  logFactory, 
    DataFactory dataFactory,
    boolean     readOnly,
    Object      compatibilitySpace) 
    {

		super();

		this.xactFactory = xactFactory;
		this.logFactory  = logFactory;
		this.dataFactory = dataFactory;
		this.readOnly    = readOnly;

		this.compatibilitySpace = 
            (compatibilitySpace == null ? this : compatibilitySpace);

 		if (SanityManager.DEBUG)
		{
 			SanityManager.ASSERT(dataFactory != null, "datafactory is null");
 			SanityManager.ASSERT(xactFactory != null, "xactfactory is null");
 			SanityManager.ASSERT(logFactory != null, "logfactory is null");
		}


		resetDefaultLocking();

		// TransactionTable needs this
		xactFactory.setNewTransactionId((XactId)null, this);

		setIdleState();

        /*
        System.out.println("Xact.constructor: readonly = " + this.readOnly +
                ";this = " + this);
                */
	}


	/*
	** Methods of RawTransaction
	*/

	/**
	*/
	public final LockFactory getLockFactory() {
		return xactFactory.getLockFactory();
	}

	public final DataFactory getDataFactory() {
		return dataFactory;
	}

	/**
		Get cache statistics for the specified cache
	*/
	public long[] getCacheStats(String cacheName) {
		return getDataFactory().getCacheStats(cacheName);
	}

	/**
		Reset the cache statistics for the specified cache
	*/
	public void resetCacheStats(String cacheName)  {
		getDataFactory().resetCacheStats(cacheName);
	}

	/**
		Return true if any transaction is currently blocked, even if not by
		this transaction.

	 */
	public boolean anyoneBlocked() {
		return getLockFactory().anyoneBlocked();
	}

	public DynamicByteArrayOutputStream getLogBuffer() {

		if (logBuffer == null) {
			logBuffer = new DynamicByteArrayOutputStream(1024);
		} else {
			logBuffer.reset();
		}

		return logBuffer;
	}
	
	/** Log and apply a compensation operation.
		Only need to write out the compensation op itself, the optional data has already
		been written by the rollforward operation this is attempting to undo.

		@see RawTransaction#logAndDo 
		
		@exception StandardException  Standard cloudscape exception policy
	*/
	public void logAndUndo(Compensation compensation, LogInstant undoInstant, LimitObjectInput in)
		 throws StandardException
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(logStart != null);
		}

		setActiveState();

		if (state == ACTIVE)
			setUpdateState();

		seenUpdates = true;
		
		LogInstant clrInstant = logger.logAndUndo(this, compensation, undoInstant, in);

		setLastLogInstant(clrInstant);

		// set the top savepoint to rollback to this record if it doesn't yet have a point saved
		if ((savePoints != null) && !savePoints.empty()) {

			SavePoint sp = (SavePoint) savePoints.peek();
			if (sp.getSavePoint() == null)
				sp.setSavePoint(clrInstant);
		}
	}

	/** 
		Add this to the xactFactory list of update transaction.
		@param rollbackFirst true if this transaction should be rolled back
				first during recovery
	*/
	public void addUpdateTransaction(int transactionStatus)
	{
		// during runtime, rollbackFirst == recoveryRolblackFirst(), but during
		// recovery redo, we only use a regular transaction, so we need to get
		// the rollbackFirst status from the log record
		//
		// If my Id is null, I have no identity, makes no sense to add it to a
		// transaction table where my identity will be saved and restored
		if (myId != null)
			xactFactory.addUpdateTransaction(myId, this, transactionStatus);

	}

	/** Remove this from the xactFactory list of update transaction. */
	public void removeUpdateTransaction()
	{
		if (myId != null)
			xactFactory.removeUpdateTransaction(myId);

		// If my Id is null, I have no identity, makes no sense to remove it
		// from transaction table
	}

	/** Remove this from the xactFactory list of update transaction. */
	public void prepareTransaction()
	{
        // RESOLVE - should I be changing the state to PREPARE?

		if (myId != null)
        {
            // If my Id is null, I have no identity, makes no sense to set
            // my state in the transaction table.

			xactFactory.prepareTransaction(myId);
        }
	}

	/**
		Set the log instant for the first log record written by this transaction.
	*/
	public void setFirstLogInstant(LogInstant instant)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(instant != null);
			SanityManager.ASSERT(logStart == null);
		}

		logStart = instant;
	}

	/**
		Get the log instant for the first log record written by this transaction.
	*/
	public LogInstant getFirstLogInstant()
	{
		return logStart;
	}

	/**
		Set the log instant for the last log record written by this transaction. 
	*/
	public void setLastLogInstant(LogInstant instant)
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(instant != null);
		}

		logLast = instant;
	}

	/**
		Get the log instant for the last log record written by this transaction. 
	*/
	public LogInstant getLastLogInstant()
	{
		return logLast;
	}

	/**
		Set my transaction identifier.
	*/
	public void setTransactionId(GlobalTransactionId extid, TransactionId localid) {

		if (SanityManager.DEBUG) {

			//SanityManager.ASSERT(myGlobalId == null, "my globalId is not null");
            if (!(state == IDLE || state == Xact.ACTIVE || 
                  (state== CLOSED && justCreated)))
            {
                SanityManager.THROWASSERT(
                    "my state is not idle nor active " + state);
            }
		}

		myGlobalId = extid;
		myId = localid;

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace") && extid != null)
            {
				SanityManager.DEBUG(
                    "XATrace","setting xid: " + myId + " " + myGlobalId 
							   + " state " + state + " " + this);

                SanityManager.showTrace(new Throwable());
                // Thread.dumpStack();
            }
		}

	}

	public void setTransactionId(Loggable beginXact, TransactionId localId)
	{
		if (SanityManager.DEBUG) {
			// SanityManager.ASSERT(myId == null);
			SanityManager.ASSERT((state == IDLE) || (state == ACTIVE));
			SanityManager.ASSERT(beginXact instanceof BeginXact);
		}

		myId = localId;
		myGlobalId = ((BeginXact)beginXact).getGlobalId();
	}

	/*
	** Methods of Transaction
	*/

	/**
		The default value for LOCKS_ESCALATION_THRESHOLD
		@exception StandardException  Standard cloudscape exception policy
	 */
	public void setup(PersistentSet set)
		throws StandardException {

		int escalationThreshold = PropertyUtil.getServiceInt(set,
			Property.LOCKS_ESCALATION_THRESHOLD,
			Property.MIN_LOCKS_ESCALATION_THRESHOLD,
			Integer.MAX_VALUE,
			Property.DEFAULT_LOCKS_ESCALATION_THRESHOLD);


		getLockFactory().setLimit(this, this, escalationThreshold, this);

	}

	/**
		get the Global (external to raw store) transaction id that is unique
		across all raw stores
	*/
	public final GlobalTransactionId getGlobalId() 
    {

		return myGlobalId;
	}

	public final ContextManager getContextManager() 
    {
		return(xc.getContextManager());
	}

    /**
     * Get the compatibility space of the transaction.
     * <p>
     * Returns an object that can be used with the lock manager to provide
     * the compatibility space of a transaction.  2 transactions with the
     * same compatibility space will not conflict in locks.  The usual case
     * is that each transaction has it's own unique compatibility space.
     * <p>
     *
	 * @return The compatibility space of the transaction.
     **/
    public Object getCompatibilitySpace()
    {
        if (SanityManager.DEBUG)
        {
			SanityManager.ASSERT(
                compatibilitySpace != null, 
                "cannot have a null compatibilitySpace.");
        }

        return(this.compatibilitySpace);
    }


	/**
		get the short (internal to raw store) transaction id that is unique
		only for this raw store
	*/
	public final TransactionId getId() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                myId != null, "cannot have a transaction with null id");

		return myId;
	}

	/**
		Get the transaction id without sanity check, this should only be called
		by a cloned TransactionTableEntry 
	 */
	protected final TransactionId getIdNoCheck()
	{
		return myId;
	}

	/**
		Get my transaction context Id
	*/
	public final String getContextId() 
	{
		return (xc == null) ? null : xc.getIdName();
	}


	/**
		Get the current default locking policy for all operations within this
		transaction. The transaction is initially started with a default
		locking policy equivalent to
		<PRE>
			 newLockingPolicy(
              LockingPolicy.MODE_RECORD, TransactionController.ISOLATION_SERIALIZABLE, true);
		</PRE>
        This default can be changed by subsequent calls to 
        setDefaultLockingPolicy(LockingPolicy policy).

	    @see Transaction#getDefaultLockingPolicy


		@return The current default locking policy in this transaction.
	*/

	public LockingPolicy getDefaultLockingPolicy()
    {
        return(defaultLocking);
    }


	/** @see Transaction#newLockingPolicy */
	public final LockingPolicy newLockingPolicy(int mode, int isolation, boolean stricterOk) {

		return xactFactory.getLockingPolicy(mode, isolation, stricterOk);

	}

	/** @see Transaction#setDefaultLockingPolicy */
	public final void setDefaultLockingPolicy(LockingPolicy policy) {

		if (policy == null)
			policy = xactFactory.getLockingPolicy(LockingPolicy.MODE_NONE, TransactionController.ISOLATION_NOLOCK, false);
		defaultLocking = policy;
	}
	
	/** 
	  @exception StandardException  Standard cloudscape exception policy
	*/
	public LogInstant commit() throws StandardException
	{
		return commit(COMMIT_SYNC);
	}

	/** 
	  @exception StandardException  Standard cloudscape exception policy
	*/
	public LogInstant commitNoSync(int commitflag) throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			int checkflag = Transaction.RELEASE_LOCKS|Transaction.KEEP_LOCKS;

			SanityManager.ASSERT((commitflag & checkflag) != 0, 
			  "commitNoSync must specify whether to keep or release locks");

			SanityManager.ASSERT((commitflag & checkflag) != checkflag,
			  "cannot set both RELEASE and KEEP LOCKS flag"); 

            if ((commitflag & 
                 TransactionController.READONLY_TRANSACTION_INITIALIZATION) 
                    != 0)
            {
                SanityManager.ASSERT((state == IDLE) || (state == ACTIVE));
            }
		}

		// Short circuit commit no sync if we are still initializing the
		// transaction.  Before a new transaction object is returned to the
		// user, it is "commit'ed" many times using commitNoSync with 
		// TransactionController.READONLY_TRANSACTION_INITIALIZATION flag to
		// release read locks and reset the transaction state back to Idle.  
		// If nothing has actually happened to the transaction object, return
		// right away and avoid the cost of going thru the commit logic.
		//
		if (state == IDLE && savePoints == null && 
			((commitflag & TransactionController.READONLY_TRANSACTION_INITIALIZATION) != 0))
			return null;

		return commit(COMMIT_NO_SYNC | commitflag);
	}

	/** 
	  @exception StandardException  Standard cloudscape exception policy
	  @see Transaction#commit
	*/

    /**
     * Do work of commit that is common to xa_prepare and commit.
     * <p>
     * Do all the work necessary as part of a commit up to and including
     * writing the commit log record.  This routine is used by both prepare
     * and commit.  The work post commit is done by completeCommit().
     * <p>
     *
     * @param commitflag various flavors of commit.
     *
	 * @exception  StandardException  Standard exception policy.
	 * @see Transaction#commit
     **/
	private LogInstant prepareCommit(int commitflag) 
        throws StandardException 
    {
		LogInstant flushTo = null;

		if (state == CLOSED)
        {
			throw StandardException.newException(
                    SQLState.XACT_PROTOCOL_VIOLATION);
        }

		if (SanityManager.DEBUG)
		{
			if ((commitflag & Transaction.KEEP_LOCKS) != 0)
			{
                // RESOLVE (mikem) - prepare actually want's to keep locks
                // during a prepare.
				SanityManager.ASSERT(
                    (((commitflag & COMMIT_NO_SYNC) != 0) || 
                     ((commitflag & COMMIT_PREPARE) != 0)),
                    "can keep locks around only in commitNoSync or prepare"); 

				SanityManager.ASSERT(
                    isUserTransaction(),
                    "KEEP_LOCKS can only be set on user transaction commits");
			}
		}


		try {

			preComplete(COMMIT);

			// flush the log.

			if (seenUpdates) {

				EndXact ex = 
                    new EndXact(
                        getGlobalId(), 
                        ((commitflag & COMMIT_PREPARE) == 0 ? 
                             END_COMMITTED : END_PREPARED)
                                | statusForEndXactLog());

				flushTo = logger.logAndDo(this, ex);

				if (xactFactory.flushLogOnCommit(xc.getIdName()))
				{
					if ((commitflag & COMMIT_SYNC) == 0)
                    {
                        // not flushing the log right now, subsequent commit
                        // will need to flush the log
						needSync = true; 
                    }
					else
					{
						logger.flush(flushTo);
						needSync = false;
					}
				}
			}
			else if (needSync && (commitflag & COMMIT_SYNC) != 0)
			{
				// this transaction object was used to lazily commit some
				// previous transaction without syncing.  Now that we commit
				// for real, make sure any outstanding log is flushed.
				logger.flushAll();
				needSync = false;
			}
		} 
        catch (StandardException se) 
        {

			// This catches any exceptions that have Transaction severity
			// or less (e.g. Statement exception). If we received any lesser
			// error then we abort the transaction anyway.

			if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
            {
				throw StandardException.newException(
                        SQLState.XACT_COMMIT_EXCEPTION, se);
            }

			throw se;

		}
		return flushTo;
	}

    /**
     * Do work to complete a commit which is not just a prepare.
     * <p>
     * Releases locks, does post commit work, and moves the state of the
     * transaction to IDLE.
     * <p>
     *
     * @param commitflag various flavors of commit.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	private void completeCommit(int commitflag) 
        throws StandardException 
    {
		// this releases our logical locks if commitflag don't have KEEP_LOCKS.
		postComplete(commitflag, COMMIT);

		// this transfer postCommitWorks to PostCommit queue
		if ((commitflag & Transaction.KEEP_LOCKS) == 0)
		{
			// if locks are released, start post commit processing
			postTermination();
		}
		else
		{
			// RESOLVE: actually, this transaction may not have outstanding
			// locks.  It didn't release them, but that doesn't mean it has got
			// them.  This is mostly harmless.

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(myGlobalId == null,
				 "calling commit with KEEP_LOCKS on a global transaction");

			// we have unreleased locks, the transaction has resource and
			// therefore is "active"
			setActiveState();
		}

		myGlobalId = null;
		return;
	}

	/** 
	  @exception StandardException  Standard cloudscape exception policy
	  @see Transaction#commit
	*/
	private LogInstant commit(int commitflag) 
        throws StandardException 
    {
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","commiting ");
		}

        LogInstant flushTo = prepareCommit(commitflag);

        completeCommit(commitflag);

        return(flushTo);
	}


	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#abort
	*/
	public void abort() throws StandardException {

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","aborting ");
		}

		if (state == CLOSED)
		{
			// I would have leave this in but close() nulls out myGlobalId
			// if (myGlobalId == null)
            // {
			//   throw StandardException.newException(
            //           SQLState.XACT_PROTOCOL_VIOLATION);
            // }

			if (SanityManager.DEBUG)
			{
				// Only global transaction is allowed to abort a closed
				// transaction.
				if (!sanityCheck_xaclosed)
                {
					throw StandardException.newException(
                            SQLState.XACT_PROTOCOL_VIOLATION);
                }
			}

			// In global transaction, the xact object is closed automatically
			// on a transaction level rollback.  This cause error handling to
			// fail because when upper level contexts in the context manager
			// unwinds, it calls abort again, which would have caused a
			// protocol violation.
			return;
		}

		/* This routine is never called by recovery redo, only by runtime and
		   recovery undo. During recovery undo, even though no log record has
		   been written by this session, it still need to rollback the
		   incomplete transaction.

		   The way to tell if this trasanction has ever written a log record is
		   by FirstLogInstant.
		*/

		try {
			preComplete(ABORT);

			// rollback the log - if logger is null, nothing I can do, crash.
			if (getFirstLogInstant() != null) {
				if (logger == null)
                {
					throw StandardException.newException(SQLState.XACT_CANNOT_ABORT_NULL_LOGGER);
                }

				logger.undo(
                    this, getId(), getFirstLogInstant(), getLastLogInstant());

				EndXact ex = new EndXact(getGlobalId(),
										 END_ABORTED | statusForEndXactLog());

				logger.flush(logger.logAndDo(this, ex));
			}
			else if (needSync)
			{
				// this transaction object was used to lazily commit some
				// previous transaction without syncing.  Now that we abort
				// for real, make sure any outstanding log is flushed.
				logger.flushAll();
			}

			needSync = false;
			
		} catch (StandardException se) {

			// This catches any exceptions that have System severity
			// or less (e.g. Statement exception).
			//
			// If we have any error during an undo we just shut the system
			// down, this is a bit drastic but it does ensure that the database
			// will not become corrupted by changes that see half committed 
            // changes.
            //
			// Note that we do not release our locks if we come thorugh this
			// path, if we did then another transaction could complete before
			// the system shuts down and make changes based upon the changes
			// that we couldn't back out.

			if (se.getSeverity() < ExceptionSeverity.SYSTEM_SEVERITY)
            {
				throw logFactory.markCorrupt(
                    StandardException.newException(
                        SQLState.XACT_ABORT_EXCEPTION, se));
            }

			throw se;

		}

		// this releases our locks.
		postComplete(0, ABORT);

		// get rid of all post commit work - we aborted, therefore no post
		// commit work
		if (postCommitWorks != null && !postCommitWorks.isEmpty())
		{
			postCommitWorks.clear();
		}

		// Now do post termination work - must do this after the rollback is
		// complete because the rollback itself may generate postTermination
		// work.
		postTermination();

		myGlobalId = null;
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
     * This routine is only called during Recovery.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void reprepare()
		throws StandardException
    {
		if (state == CLOSED)
        {
			throw StandardException.newException(
                    SQLState.XACT_PROTOCOL_VIOLATION);
        }

        // Should only be called during recovery on global transactions, 
        // after redo and undo.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(myGlobalId != null);
            SanityManager.ASSERT(state == PREPARED);
        }

		try 
        {
            if (logger == null)
            {
                throw StandardException.newException(
                        SQLState.XACT_CANNOT_ABORT_NULL_LOGGER);
            }

            // temporarily set state back to UPDATE, so that the reprepare()
            // call can do operations on the xact that would "normally" be 
            // disallowed on a prepared xact - like opening a container to
            // lock it.

            state = UPDATE;

            // re-prepare the transaction.
            logger.reprepare(
                this, getId(), getFirstLogInstant(), getLastLogInstant());

            // make sure the xact is prepare state when we are done.
            state = PREPARED;

            seenUpdates = true;

		} catch (StandardException se) {

			// This catches any exceptions that have System severity
			// or less (e.g. Statement exception).
			//
			// If we have any error during an reprepare we just shut the system
			// down, this is a bit drastic but it does ensure that the database
			// will not become corrupted by changes that see data that is part
            // of a prepared transaction.
            //
			// Note that we do not release our locks if we come thorugh this
			// path, if we did then another transaction could complete before
			// the system shuts down and make changes based upon the changes
			// that we couldn't back out.

			if (se.getSeverity() < ExceptionSeverity.SYSTEM_SEVERITY)
            {
				throw logFactory.markCorrupt(
                    StandardException.newException(
                        SQLState.XACT_ABORT_EXCEPTION, se));
            }

			throw se;
		}

        // RESOLVE - something needs to change the state of the XACT so that
        // it is not recovery state anymore?
	}

	/**
        If this transaction is not idle, abort it.  After this call close().

		@exception StandardException Standard Cloudscape error policy
        Thrown if the transaction is not idle.

		
	*/
	public void destroy() throws StandardException 
    {
        if (state != CLOSED)
            abort();

        close();
    }

	/**
	    @exception StandardException  Standard cloudscape exception policy
		@exception StandardException Thrown if the transaction is not idle, the
		transaction remains open.
		@see Transaction#close

		@exception StandardException	Standard cloudscape policy
	*/
	public void close() throws StandardException {


        /*

        if (((LogToFile) logFactory).inRedo)
        {
            SanityManager.showTrace(new Throwable());
            SanityManager.THROWASSERT("in Redo while in close");
        }
        */

		switch (state) {
		case CLOSED:
			return;
		case IDLE:
			break;
		default:
			throw StandardException.newException(
                SQLState.XACT_TRANSACTION_NOT_IDLE);
		}

		if (SanityManager.DEBUG) {

			SanityManager.ASSERT(xc.getTransaction() == this);

			SanityManager.ASSERT(
                (postCommitWorks == null || postCommitWorks.isEmpty()),
                "cannot close a transaction with post commit work pending");

			// use this for sanity checking
			if (myGlobalId != null)
				sanityCheck_xaclosed = true;
		}

		getLockFactory().clearLimit(this, this);

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","closing " + myId + " " + myGlobalId);
			// Thread.dumpStack();
		}

		// if we just finished recovery myId could be null
		if (myId != null)
			xactFactory.remove((XactId)myId);		

		xc.popMe();
		xc = null;

		myGlobalId = null;
		myId = null;
		logStart = null;
		logLast = null;



		/* MT - no need to synchronize it, the state is current IDLE which will
		 * return the same result to isActive() as if it is CLOSED
		 */
		state = CLOSED;

	}

	/** 
		Log the operation and do it.

		If this transaction has not generated any log records prior to this,
		then log a beginXact log record.

		If the passed in operation is null, then do nothing (after logging the
		beginXact if needed).

	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#logAndDo 
	*/
	public void logAndDo(Loggable operation) throws StandardException {

		LogInstant instant = null;

		if (logger == null)
			getLogger();

		if (logger == null)
        {
			throw StandardException.newException(
                    SQLState.XACT_CANNOT_LOG_CHANGE);
        }

		setActiveState();

		if (state == ACTIVE)
		{
			instant = 
                logger.logAndDo(
                    this, 
                    new BeginXact(getGlobalId(), statusForBeginXactLog()));

			setUpdateState();
		}
		seenUpdates = true;

		if (operation != null)
		{
			instant = logger.logAndDo(this, operation);
			if (instant != null) {
				setLastLogInstant(instant);

				if ((savePoints != null) && !savePoints.empty()) {
					for (int i = savePoints.size() - 1; i >= 0; i--) {
					    // set the top savepoint to rollback to this record if
                        // it doesn't yet have a point saved

						SavePoint sp = (SavePoint) savePoints.elementAt(i);
						if (sp.getSavePoint() == null) {
							sp.setSavePoint(instant);
						} else
							break;
					}
				}
			}

		}
		else
		{
			if (instant != null)
				setLastLogInstant(instant);
		}

	}

	public void addPostCommitWork(Serviceable work)
	{
		if (recoveryTransaction)
			return;

		if (postCommitWorks == null)
			postCommitWorks = new ArrayList(1);
		postCommitWorks.add(work);
	}

	public void addPostTerminationWork(Serviceable work)
	{
		if (recoveryTransaction)
			return;

		if (postTerminationWorks == null)
			postTerminationWorks = new ArrayList(2);
		postTerminationWorks.add(work);
	}


	/**
		Return a record handle that is initialized to the given page number and
        record id.

		@exception StandardException Standard cloudscape exception policy.

		@param segmentId    segment where the RecordHandle belongs.
		@param containerId  container where the RecordHandle belongs.
		@param pageNumber   the page number of the RecordHandle.
		@param recordId     the record id of the RecordHandle.

		@see RecordHandle
	*/
//	public RecordHandle makeRecordHandle(long segmentId, long containerId, long pageNumber, int recordId)
//		 throws	StandardException
 //    {
//         return(this.dataFactory.makeRecordHandle(
//             segmentId, containerId, pageNumber, recordId));
//     }

	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#openContainer 
	*/
	public ContainerHandle openContainer(ContainerKey containerId,  int mode)
		throws StandardException {

		return openContainer(containerId, defaultLockingPolicy(), mode);
	}

	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#openContainer 
	*/
	public ContainerHandle openContainer(ContainerKey containerId, LockingPolicy locking, int mode)
		throws StandardException {

		setActiveState();

		if (locking == null)
			locking = xactFactory.getLockingPolicy(LockingPolicy.MODE_NONE, TransactionController.ISOLATION_NOLOCK, false);

		return dataFactory.openContainer(this, containerId, locking, mode);
	}

	/**
		Open a container that may already have been dropped.

		@exception StandardException  Standard cloudscape exception policy
		@see RawTransaction#openDroppedContainer
	*/
	public RawContainerHandle openDroppedContainer(ContainerKey containerId, LockingPolicy locking)
		 throws StandardException
	{
		setActiveState();

		if (locking == null)
			locking = xactFactory.getLockingPolicy(LockingPolicy.MODE_NONE, TransactionController.ISOLATION_NOLOCK, false);

		RawContainerHandle hdl = null;

		// first try to open it for update, if that fail, open it for read
		try
		{
			hdl = dataFactory.openDroppedContainer(this, containerId, locking,
											ContainerHandle.MODE_FORUPDATE);
		}
		catch (StandardException se)
		{
			// if this also fail, throw exception
			hdl = dataFactory.openDroppedContainer(this, containerId,
											locking,
											ContainerHandle.MODE_READONLY);
		}

		return hdl;
	}


	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#addContainer 
	*/
	public long addContainer(long segmentId, long containerid, int mode, Properties tableProperties, int temporaryFlag)
		throws StandardException {

		setActiveState();

		return dataFactory.addContainer(this, segmentId, containerid, mode, tableProperties, temporaryFlag);
	}

	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#addAndLoadStreamContainer
	*/
	public long addAndLoadStreamContainer(long segmentId, Properties tableProperties, RowSource rowSource)
		throws StandardException {

		setActiveState();

		return dataFactory.addAndLoadStreamContainer(this, segmentId, tableProperties, rowSource);

	}

	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#openStreamContainer
	*/
	public StreamContainerHandle openStreamContainer(
    long    segmentId, 
    long    containerId,
    boolean hold)
		throws StandardException 
    {
		setActiveState();

		return(
            dataFactory.openStreamContainer(
                this, segmentId, containerId, hold));
	}

	/**
		@see Transaction#dropStreamContainer
		@exception StandardException Standard Cloudscape error policy
	*/
	public void dropStreamContainer(long segmentId, long containerId)
		throws StandardException {

		setActiveState();

		dataFactory.dropStreamContainer(this, segmentId, containerId); 
	}

	/**
		Recreate a container during load tran - use only by media recovery.

		@exception StandardException  Standard cloudscape exception policy
		@see RawTransaction#reCreateContainerForLoadTran
	 */
	public void reCreateContainerForLoadTran
		(long segmentId, long containerId, ByteArray containerInfo)
		throws StandardException
	{
		setActiveState();

		dataFactory.reCreateContainerForLoadTran(
			this, segmentId, containerId, containerInfo);
	}

	/**
		@see Transaction#dropContainer
		@exception StandardException Standard Cloudscape error policy
	*/
	public void dropContainer(ContainerKey containerId)
		throws StandardException {

		setActiveState();

		dataFactory.dropContainer(this, containerId); 
	}

	/**
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#setSavePoint
	*/
	public int setSavePoint(String name, Object kindOfSavepoint) 
        throws StandardException 
    {

		if (kindOfSavepoint != null && kindOfSavepoint instanceof String)  
        {
            //that means we are trying to set a SQL savepoint

            //error if this SQL savepoint is getting nested into other user 
            // defined savepoints
			throwExceptionIfSQLSavepointNotAllowed(kindOfSavepoint);
        }

		// while setting a savepoint, we just want to see if there is a 
        // savepoint with the passed name already in the system.
		if (getSavePointPosition(name, kindOfSavepoint, false) != -1)
        {
			throw StandardException.newException(
                    SQLState.XACT_SAVEPOINT_EXISTS);
        }

		if (savePoints == null)
			savePoints = new Stack();

		savePoints.push(new SavePoint(name, kindOfSavepoint));

		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (savePoints.size() > 20)
					System.out.println("memoryLeakTrace:Xact:savepoints " + savePoints.size());
			}
		}
		return savePoints.size();
	}

	// SQL savepoint can't be nested inside other user defined savepoints. To 
    // enforce this, we check if there are already user savepoint(SQL/JDBC)
    // defined in the transaction. If yes, then throw an exception

	/**
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#setSavePoint
	*/
	private void throwExceptionIfSQLSavepointNotAllowed(Object kindOfSavepoint)
        throws StandardException 
    {

		boolean foundUserSavepoint = false;

		if ((savePoints != null) && !savePoints.empty()) {
			for (int i = savePoints.size() - 1; i >= 0; i--) {
				SavePoint sp = (SavePoint) savePoints.elementAt(i);
				if (sp.isThisUserDefinedsavepoint()) 
				{
                    //found a user defined savepoint

					foundUserSavepoint = true;
					break;
				}
			}
		}

		if (foundUserSavepoint) 
			throw StandardException.newException(
                    SQLState.XACT_MAX_SAVEPOINT_LEVEL_REACHED);
	}

	/**
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#releaseSavePoint
	*/
	public int releaseSavePoint(String name, Object kindOfSavepoint) 
        throws StandardException 
    {
		int position = getSavePointPosition(name, kindOfSavepoint, true);

		if (position == -1)
        {
			// following means this is a JDBC savepoint. 
            // We prepend i./e. to JDBC savepoint names in JDBC layer.
			// Need to trim that here before giving error

			if (kindOfSavepoint != null && !(kindOfSavepoint instanceof String))
            {
                // this means this is a JDBC savepoint. 
                // We append "i."/"e." to JDBC savepoint names. 
                // Trimming that here before giving error

				name = name.substring(2); 
            }
			throw StandardException.newException(
                        SQLState.XACT_SAVEPOINT_NOT_FOUND, name);
        }

		popSavePoints(position, true);
		return savePoints.size();
	}

	/** 
	    @exception StandardException  Standard cloudscape exception policy
		@see Transaction#rollbackToSavePoint
	*/
	public int rollbackToSavePoint(String name, Object kindOfSavepoint) 
        throws StandardException 
    {
		int position = getSavePointPosition(name, kindOfSavepoint, true);

		if (position == -1)
        {
			// following means this is a JDBC savepoint. 
            // We append i./e. to JDBC savepoint names in JDBC layer.
			// Need to trim that here before giving error
			if (kindOfSavepoint != null && !(kindOfSavepoint instanceof String))
				name = name.substring(2);
			throw StandardException.newException(
                        SQLState.XACT_SAVEPOINT_NOT_FOUND, name);
        }

		notifyObservers(SAVEPOINT_ROLLBACK);

		popSavePoints(position, false);
		return savePoints.size();
	}


	/*
	**	Implementation specific methods
	*/

	/**
		Get the Logger object used to write log records to the transaction log.
	*/
	private void getLogger() {

		logger = logFactory.getLogger();
	}


	/**
		Transform this identity to the one stored in transaction table entry.
		Used by recovery only!
	*/
	protected void assumeIdentity(TransactionTableEntry ent)
	{
		if (ent != null)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(ent.getXid() != null, "TTE.xid is null");

				SanityManager.ASSERT(
                    ent.getFirstLog() != null, "TTE.firstLog is null");
			}

			// I am the transaction that is using this TransactionTableEntry
			ent.setXact(this);

			myId     = ent.getXid();
			logStart = ent.getFirstLog();
			logLast  = ent.getLastLog();


            // This routine is only used by recovery to assume the identity
            // of the transaction for each log record during redo and undo.
            // For this purpose the transaction should act like a local 
            // transaction, and ignore the fact that it may or may not be
            // an XA global transaction - this is necessary as global 
            // transactions can only be committed or aborted once, but 
            // recovery needs to reuse the same xact over and over again.  
            // For this purpose set myGlobalId to null so it is treated as
            // a local xact - the entry in the transaction table will 
            // track that in reality it is a global id and remember it's
            // value.
			myGlobalId = null;

			// I am very active
			if (state == IDLE)
				state = ACTIVE;

			if (SanityManager.DEBUG)
			{
				if (state != ACTIVE && state != UPDATE && state != PREPARED)
                    SanityManager.THROWASSERT(
                        "recovery transaction have illegal state " + state +
                        "xact = " + this);
			}



			if (logger == null)
				getLogger();

			savedEndStatus = 0;
		}
		else
		{
			myGlobalId = null;
			myId = null;
			logStart = null;
			logLast = null;
			state = IDLE;
		}
	}

    /**
     * Assume complete identity of the given Transaction Table Entry.
     * <p>
     * Used by the final phase of the recovery to create new real transactions
     * to take on the identity of in-doubt prepared transactions found during
     * redo.  Need to assume the globalId.
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param old_ent The original entry we are assuming the identity of.
     * @param new_ent The new permanent entry in the transaction table.
     *
     **/
	protected void assumeGlobalXactIdentity(
    TransactionTableEntry ent)
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(ent != null);
            SanityManager.ASSERT(ent.getXid() != null, "TTE.xid is null");
            SanityManager.ASSERT(
                ent.getFirstLog() != null, "TTE.firstLog is null");
            SanityManager.ASSERT(ent.isPrepared());
        }

        myId        = ent.getXid();
        myGlobalId  = ent.getGid();
        logStart    = ent.getFirstLog();
        logLast     = ent.getLastLog();

        // I am very active
        if (state == IDLE)
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("XATrace"))
					SanityManager.DEBUG("XATrace","set active state in assume Global XactIdentity");
			}

            state = ACTIVE;
		}

        if (ent.isPrepared())
            state = PREPARED;

        // I am the transaction that is using this TransactionTableEntry
        ent.setXact(this);

        if (SanityManager.DEBUG)
        {
            if (state != ACTIVE && state != UPDATE && state != PREPARED)
                SanityManager.THROWASSERT(
                    "recovery transaction have illegal state " + state +
                    "xact = " + this);
        }

        if (logger == null)
            getLogger();

		savedEndStatus = 0;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(myGlobalId != null);

            // at least right now only prepared xact call this during recovery.
            SanityManager.ASSERT(state == PREPARED);
        }
	}


	/**
		Move the transaction into the update state.
		@exception StandardException problem setting a transaction id
	*/
	private final void setUpdateState() throws StandardException {

        /*
        System.out.println("calling setUpdateState() - readOnly = " + readOnly +
                ";this = " + this);
        System.out.println("calling setUpdateState():");
                (new Throwable()).printStackTrace();
        */

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                state == ACTIVE, 
                "setting update state without first going thru ACTIVE state");

			SanityManager.ASSERT(
                myId != null, 
                "setting update state to a trasnaction with Null ID");
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
            {
				SanityManager.DEBUG("XATrace","set update state");
                SanityManager.showTrace(new Throwable());
            }
		}

        if (readOnly)
        {
            throw StandardException.newException(
                SQLState.XACT_PROTOCOL_VIOLATION);
        }

		state = UPDATE;
	}

	protected void setIdleState() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(myId != null, "setIdleState got null ID");

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("TranTrace"))
            {
                SanityManager.DEBUG(
                    "TranTrace", "transaction going idle " + myId);

                SanityManager.showTrace(new Throwable("TranTrace"));
            }
		}


		/* MT - single thread throught synchronizing this.  Even though no other
		 * thread can call this, they may call isActive which needs to be
		 * synchronized with state transaction into or out of the idle state
		 */
		// synchronized(this) -- int access, implicit synchronization
		// due to atomic action
		{
			state = IDLE;
		}

		

		seenUpdates = false;

		// these fields will NOT be accessed by the checkpoint thread at the
		// same time because the doMe method of EndXact removed this
		// transaction from the "update transaction" list, therefore when the
		// checkpoint writes out the transaction table, this transaction will
		// be skipped.  OK to just change it without synchronization here.

		logStart = null;
		logLast = null;

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","set idle state : " + state + " " + this);
		}

	}

	protected final void setActiveState() throws StandardException {

		if ((state == CLOSED) || (!inAbort() && (state == PREPARED)))
        {
            // This is where we catch attempted activity on a prepared xact.
			throw StandardException.newException(
                    SQLState.XACT_PROTOCOL_VIOLATION);
        }

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(myId != null, "setActiveState got null ID");

		if (state == IDLE)
		{
			synchronized(this)
			{
				state = ACTIVE;
			}

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON("XATrace"))
                {
					SanityManager.DEBUG("XATrace","set active state " + this);
                    SanityManager.showTrace(new Throwable("XATrace"));
                }
			}


			if (!justCreated)
				xactFactory.setNewTransactionId(myId, this);

			justCreated = false;

			if (SanityManager.DEBUG)
            {
				if (SanityManager.DEBUG_ON("TranTrace"))
                {
                    SanityManager.DEBUG(
                        "TranTrace", "transaction going active " + myId);

                    SanityManager.showTrace(new Throwable("TranTrace"));
                }
			}

		}
	}

    /**
     * Move the state of the transaction from UPDATE to PREPARE.
     * <p>
     * The state transition should only be from UPDATE to PREPARE.  Read-only
     * transactions (IDLE and ACTIVE) will never be prepared, they will be
     * commited when the prepare is requested.  Only Update transactions will
     * be allowed to go to prepared state.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected final void setPrepareState() 
        throws StandardException 
    {
		if (state == PREPARED || state == CLOSED)
        {
			throw StandardException.newException(
                    SQLState.XACT_PROTOCOL_VIOLATION);
        }

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                state == UPDATE, 
                "setting PREPARED state without first going thru UPDATE state");
			SanityManager.ASSERT(
                myId != null, 
                "setting PREPARED state to a transaction with Null ID");
		}

		state = PREPARED;
	}


	public final LockingPolicy defaultLockingPolicy() {
		return defaultLocking;
	}


	private final void releaseAllLocks() {

		getLockFactory().unlockGroup(getCompatibilitySpace(), this);
	}

	void resetDefaultLocking() {

		setDefaultLockingPolicy(
			newLockingPolicy(LockingPolicy.MODE_RECORD, TransactionController.ISOLATION_SERIALIZABLE, true));

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(defaultLocking != null);
		}

	}
	
	protected void preComplete(Integer commitOrAbort) throws StandardException {
		
		/* If a transaction is in COMMIT/ABORT at this point, most probably
		 * some thing went wrong in earlier attempt to commit or abort,
		 * so we don't know wther the log records got written in previous
		 * attempt. It's is better to bring down the system than make recovery
		 * fail with a duplicate log records of COMMIT/ABORT for the same Transaction.
		 */
		if (inComplete != null)
			if (commitOrAbort.equals(COMMIT))
				throw logFactory.markCorrupt(
						 StandardException.newException(SQLState.XACT_COMMIT_EXCEPTION));
			else
				throw logFactory.markCorrupt(
						 StandardException.newException(SQLState.XACT_ABORT_EXCEPTION));
		
		inComplete = commitOrAbort;
		if (!postCompleteMode)
			doComplete(commitOrAbort);

	}

	protected void postComplete(int commitflag, Integer commitOrAbort) throws StandardException {

		if (postCompleteMode)
			doComplete(commitOrAbort);

		// if we are want to commitNoSync with KEEP_LOCKS flag set, don't
		// release any locks
		if ((commitflag & Transaction.KEEP_LOCKS) == 0)
        {
			releaseAllLocks();
        }
		else 
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(commitOrAbort.equals(COMMIT),
				 "cannot keep locks around after an ABORT");
			}
		}

		setIdleState();

		inComplete = null;
	}

	protected void doComplete(Integer commitOrAbort) throws StandardException {

		// throw away all our savepoints
		if (savePoints != null)
			savePoints.removeAllElements();

		// notify any of our observers that we are completing.
		notifyObservers(commitOrAbort);

		checkObserverException();

		if (SanityManager.DEBUG) 
        {
			if (countObservers() != 0)
            {
				System.out.println(
                    "There should be 0 observers, but we still have "
					+ countObservers() + " observers.");
				notifyObservers(null);
            }
		}
	}

	private void checkObserverException() throws StandardException {
		if (observerException != null) {
			StandardException se = observerException;
			observerException = null;
			throw se;
		}
	}

	/**
	  If this is a user transaction (not an internal or nested top
	  transaction), and this is not already taking care of post
	  commit work, and not an XA transaction, then take care of hi prioirty 
      work right now using this thread and this context manager.  
      Otherwise, leave it to the post commit daemon.
	  */
	protected boolean doPostCommitWorkInTran()
	{
		return (!inPostCommitProcessing &&
				!recoveryTransaction    &&
                isUserTransaction()     &&
                (myGlobalId == null));
	}

	public boolean handlesPostTerminationWork()
	{
		// recovery transaction cannot handle post termination work
		return (recoveryTransaction == false);
	}

	public void recoveryTransaction()
	{
		recoveryTransaction = true;

		// remove myself from the transaction table because I am really a
		// "fake" transaction.  All interaction I have with the transaction
		// table should happen after I have assumed the identity of one of the
		// recovery transaction that has its state frozen in the transaction
		// table. 
		xactFactory.remove(myId);

	}


	private final void postTermination() throws StandardException
	{
		// move all the postTermination work to the postCommit queue
		int count = (postTerminationWorks == null) ? 
			0 : postTerminationWorks.size(); 

		for (int i = 0; i < count; i++)
			addPostCommitWork((Serviceable)postTerminationWorks.get(i));

		if (count > 0)
			postTerminationWorks.clear();


		// if there are post commit work to be done, transfer them to the
		// daemon.  The log is flushed, all locks released and the
		// transaction has ended at this point.
		if (postCommitWorks != null && !postCommitWorks.isEmpty())
		{
			int pcsize = postCommitWorks.size();
			
			// do we want to do post commit work with this transaction object?
			if (doPostCommitWorkInTran())
			{
				try
				{
					inPostCommitProcessing = true;

					// to avoid confusion, copy the post commit work to an array if this
					// is going to do some work now
					Serviceable[] work = new Serviceable[pcsize];
					work = (Serviceable[])postCommitWorks.toArray(work);

					// clear this for post commit processing to queue its own post
					// commit works - when it commits, it will send all its post
					// commit request to the daemon instead of dealing with it here.
					postCommitWorks.clear();

					//All the post commit work that is part  of the database creation
					//should be done on this thread immediately.
					boolean doWorkInThisThread = xactFactory.inDatabaseCreation();

					for (int i = 0; i < pcsize; i++)
					{

						//process work that should be done immediately or
						//when we  are in still in database creattion.
						//All the other work should be submitted 
						//to the post commit thread to be processed asynchronously
						if (doWorkInThisThread || work[i].serviceImmediately())
						{
							try
							{
								// this may cause other post commit work to be
								// added.  when that transaction commits, those
								// work will be transfered to the daemon
								if (work[i].performWork(xc.getContextManager()) == Serviceable.DONE)
									work[i] = null;

								// if REQUEUE, leave it on for the postcommit
								// daemon to handle
							}
							catch (StandardException se)
							{
								// don't try to service this again
								work[i] = null;

								// try to handle it here.  If we fail, then let the error percolate.
								xc.cleanupOnError(se);
							}
						}

						// either it need not be serviedASAP or it needs
						// requeueing, send it off.   Note that this is one case
						// where a REQUEUE ends up in the high priority queue.
						// Unfortunately, there is no easy way to tell.  If the
						// Servicable is well mannered, it can change itself from
						// serviceASAP to not serviceASAP if it returns REQUEUE.
						if (work[i] != null)
						{
							boolean needHelp = xactFactory.submitPostCommitWork(work[i]);
							work[i] = null;
							if (needHelp)
								doWorkInThisThread = true;
						}
					}
				}
				finally
				{
					inPostCommitProcessing = false;

					// if something untoward happends, clear the queue.
					if (postCommitWorks != null)
						postCommitWorks.clear();
				}

			}
			else
			{
				// this is for non-user transaction or post commit work that is
				// submitted in PostCommitProcessing.  (i.e., a post commit
				// work submitting other post commit work)
				for (int i = 0; i < pcsize; i++)
				{
					// SanityManager.DEBUG_PRINT("PostTermination",postCommitWorks.elementAt((i)).toString());
					xactFactory.submitPostCommitWork((Serviceable)postCommitWorks.get((i)));
				}
			}

			postCommitWorks.clear();

		}
	}

	/**
		Does a save point exist in the stack with the given name.
		Returns the position of the savepoint in the array
	*/
	private int getSavePointPosition(
    String name, 
    Object kindOfSavepoint, 
    boolean forRollbackOrRelease) 
    {
		if ((savePoints == null) || (savePoints.empty()))
			return -1;

        for (int i = savePoints.size() - 1; i >= 0; i--)
        {
            SavePoint savepoint = (SavePoint)savePoints.elementAt(i);

            if (savepoint.getName().equals(name))
            {
                if (forRollbackOrRelease && 
                    savepoint.getKindOfSavepoint() != null)
                {
                    if (savepoint.getKindOfSavepoint().equals(kindOfSavepoint))
                        return(i);
                }
                else  
                {
                    return(i);
                }
            }
        }   
		return -1;
	}

	/**
		Pop all savepoints upto the one with the given name and rollback
		all changes made since this savepoint was pushed.
		If release is true then this savepoint is popped as well,
		otherwise it is left in the stack (at the top).

		@return true if any work is rolled back, false if no work is rolled back
		@exception StandardException	Standard cloudscape policy
		@exception StandardException Thrown if a error of severity less than TransactionException#SEVERITY
		is encountered during the rollback of this savepoint.
	*/
	protected boolean popSavePoints(int position, boolean release) throws StandardException {

		if (release) {
			savePoints.setSize(position);
			return false;
		}

		LogInstant rollbackTo = null;

		int size = savePoints.size();
		for (int i = position; i < size; i++) {
			SavePoint rollbackSavePoint = (SavePoint) savePoints.elementAt(i);

			LogInstant li = rollbackSavePoint.getSavePoint();
			if (li != null) {
				rollbackTo = li;
				break;
			}
		}

		savePoints.setSize(position + 1);

		if (rollbackTo == null)
			return false;

		// now perform the rollback
		try {

			logger.undo(this, getId(), rollbackTo, getLastLogInstant());

		} catch (StandardException se) {

			// This catches any exceptions that have Transaction severity
			// or less (e.g. Statement exception). If we received any lesser
			// error then we abort the transaction anyway.

			if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
            {
				throw StandardException.newException(
                        SQLState.XACT_ROLLBACK_EXCEPTION, se);
            }

			throw se;
		}

		return true;
	}

	/**
		@exception StandardException Cloudscape Standard error policy
	 */
	public RawTransaction startNestedTopTransaction() throws StandardException {

		return xactFactory.startNestedTopTransaction(xc.getFactory(), xc.getContextManager());
	}

 	/**
 	 * see if this transaction is a user transaction.
 	 *
 	 * @return true if this transaction is a user transaction
 	 */
 	private boolean isUserTransaction()
 	{
        String context_id = getContextId();

        return(
            (context_id == XactFactory.USER_CONTEXT_ID          ||
             context_id.equals(XactFactory.USER_CONTEXT_ID)));
 	}

 	/**
 	 * see if this transaction has ever done anything.
 	 *
 	 * MT - single thread through synchronizing this.  This method may be
 	 * called by other thread to test the state of this transaction.  That's
 	 * why we need to synchronize with all methods which enters or exits the
	 * Idle state.
	 *
	 * Local method which read the state need not be synchronized because 
 	 * the other thread may look at the state but it may not change it.
 	 *
 	 * @return true if this transaction is not in idle or closed state
 	 */
 	public final boolean isActive()
 	{
		// synchronized(this) -- int access, implicit synchronization
		// due to atomic action
		int localState = state;


 		return (localState != CLOSED && localState != IDLE);
 	}

 	/**
 	 * see if this transaction is in PREPARED state.
 	 *
 	 * MT - single thread through synchronizing this.  This method may be
 	 * called by other thread to test the state of this transaction.
	 *
 	 * @return true if this transaction is in PREPARED state.
 	 */
 	public final boolean isPrepared()
 	{
		// synchronized(this) -- int access, implicit synchronization
		// due to atomic action
 		return(state == PREPARED);
 	}

	/**
		See if this transaction is in the idle state, called by other thread to
		test the state of this transaction.  That's why we need to synchronzied
		with all methods whcih enters or exits the idle state

		@return true if it is idle, otherwise false
	*/
	public boolean isIdle()
	{
		// synchronized(this) -- int access, implicit synchronization
		// due to atomic action
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","RawTran, isIdle, state = " + state);
		}

		return (state == IDLE);
	}

	/**
		see if this transaction is in a pristine state.

		<BR>MT - called only by the same thread that owns the xact, no need to synchronize.

		@return true if it hasn't done any updates, otherwise false
	*/
	public boolean isPristine()
	{
		return (state == IDLE  ||  state == ACTIVE);
	}

	public boolean inAbort() {
		return ABORT.equals(inComplete);
	}

	public FileResource getFileHandler() {
		return dataFactory.getFileHandler();
	}

 
	/**
	 * put this into the beginXact log record to help recovery
	 * if we needs to rolled back first, put that in
	 */
	protected int statusForBeginXactLog()
	{
		return  recoveryRollbackFirst() ? RECOVERY_ROLLBACK_FIRST : 0;
	}

	/**
	 * put this into the endXact log record to help recovery, 
	 * nothing to add
	 */
	protected int statusForEndXactLog()
	{
		// during recovery, the beginXact may be logged by a non-standard
		// transaction and hence the end xact it log
		// must also contain whatever a non-standard Transaction will output.  
		return savedEndStatus;
	}

	/**	
		Set the transaction to issue pre complete work at postComplete
		time, instead of preComplete time. This means that latches
		and containers will be held open until after a commit or an abort.
	*/
	void setPostComplete() {
		postCompleteMode = true;
	}


	/*
	** Lock escalation related
	*/

	/*
	** Methods of Limit
	*/

	public void reached(Object compatabilitySpace, Object group, int limit,
		Enumeration lockList, int lockCount)
		throws StandardException {

		// Count row locks by table
		Dictionary containers = new java.util.Hashtable();

		for (; lockList.hasMoreElements(); ) {

			Object plainLock = lockList.nextElement();
			if (!(plainLock instanceof RecordHandle)) {
				// only interested in rows locks
				continue;
			}

			ContainerKey ckey = ((RecordHandle) plainLock).getContainerId();
			
			LockCount lc = (LockCount) containers.get(ckey);
			if (lc == null) {
				lc = new LockCount();
				containers.put(ckey, lc);
			}
			lc.count++;
		}

		// Determine the threshold for lock escalation
		// based upon our own limit, not the current count
		int threshold = limit / (containers.size() + 1);
		if (threshold < (limit / 4))
			threshold = limit / 4;

		// try to table lock all tables that are above
		// this threshold

		boolean didEscalate = false;
		for (Enumeration e = containers.keys(); e.hasMoreElements(); ) {
			ContainerKey ckey = (ContainerKey) e.nextElement();

			LockCount lc = (LockCount) containers.get(ckey);

			if (lc.count < threshold) {
				continue;
			}

            try
            {
                if (openContainer(ckey,
                    new RowLocking3Escalate(getLockFactory()),
                    ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY |
                    ContainerHandle.MODE_FORUPDATE |
                    ContainerHandle.MODE_LOCK_NOWAIT) != null) 
                {

                    didEscalate = true;
                }
            }
            catch (StandardException se)
            {
                if (!se.getMessageId().equals(SQLState.LOCK_TIMEOUT))
                {
                    // if it is a timeout then escalate did not happen and
                    // just fall through.
                    throw se;
                }
            }
		}

		// Now notify all open containers that an escalation
		// event happened. This will cause all open row locked
		// containers to re-get their container intent locks,
		// those that are now covered by a container lock due
		// to the above escalation will move into no locking
		// mode. The open containers that were not escalated
		// will simply bump the lock count in the lock manager
		// and will not have to wait for the lock they already have.
		//
		// It would be possible to pass in the notifyObservers
		// some indication of which tables were escalated
		// to reduce the extra lock call for the un-escalated
		// containers. This would involve passing the Hashtable
		// of escalated containers and having the update method
		// of BaseContainerHandle look for its ContainerKey within it.
		if (didEscalate) {
			notifyObservers(LOCK_ESCALATE);
			checkObserverException();
		}
	}

	/**
     * Convert a local transaction to a global transaction.
     * <p>
     * Must only be called a previous local transaction was created and exists
     * in the context.  Can only be called if the current transaction is in
     * the idle state, and no current global id.  
     * <p>
     * Simply call setTransactionId() which takes care of error checking.
     *
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 **/
	public void createXATransactionFromLocalTransaction(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id)
		throws StandardException
    {
        GlobalXactId gid = new GlobalXactId(format_id, global_id, branch_id);

        if (((TransactionTable) xactFactory.getTransactionTable()).
                findTransactionContextByGlobalId(gid) != null)
        {
            throw StandardException.newException(SQLState.STORE_XA_XAER_DUPID);
        }

        setTransactionId(gid, this.getId());

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(myGlobalId != null);
    }

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 current xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_commit(
    boolean onePhase)
		throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(state != CLOSED);

        if (onePhase)
        {
            if (state == PREPARED)
            {
                throw StandardException.newException(
                        SQLState.XACT_PROTOCOL_VIOLATION);
            }

            prepareCommit(COMMIT_SYNC);

            completeCommit(COMMIT_SYNC);
        }
        else
        {
            if (state != PREPARED)
            {
                throw StandardException.newException(
                        SQLState.XACT_PROTOCOL_VIOLATION);
            }

            prepareCommit(COMMIT_SYNC);

            completeCommit(COMMIT_SYNC);
        }


        return;
    }

    /**
     * This method is called to ask the resource manager to prepare for
     * a transaction commit of the transaction specified in xid.
     * <p>
     *
     * @return         A value indicating the resource manager's vote on the
     *                 the outcome of the transaction.  The possible values
     *                 are:  XA_RDONLY or XA_OK.  If the resource manager wants
     *                 to roll back the transaction, it should do so by 
     *                 throwing an appropriate XAException in the prepare
     *                 method.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int xa_prepare()
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            if (state == CLOSED)
            {
                SanityManager.THROWASSERT(
                    "state = " + state + ";myGlobalId = " + myGlobalId); 
            }

			if (SanityManager.DEBUG_ON("XATrace"))
				SanityManager.DEBUG("XATrace","in xa_prepare, state is " + state);
		}

        if ((state == IDLE) || (state == ACTIVE))
        {
            abort();
            return(Transaction.XA_RDONLY);
        }
        else
        {
            prepareCommit(
                COMMIT_SYNC | COMMIT_PREPARE | Transaction.KEEP_LOCKS);
			//we set the following variable during prepareCommit 
			// to what we are doing, so we unset here.
			inComplete = null;

            setPrepareState();

            return(Transaction.XA_OK);
        }
    }

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_rollback()
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(state != CLOSED);

        abort();

        return;
    }

    /**
     * Return the xid as a string.
     * <p>
     * The virtual lock table depends on this routine returning just the
     * local transaction id as a string, even if it is a global transaction.
     * Joins between the lock table and the transaction table will not work
     * if this routine returns anything other than myId.toString().
     * <p>
     *
	 * @return The xid as a string.
     *
     **/
	public String toString()
	{
		// needed for virtual lock table
        try
        {
            return(myId.toString());
        }
        catch (Throwable t)
        {
            // using try/catch rather than if (myId != null) because on
            // multiple processor sometimes myId was going null after the
            // test but before the use.
            return("null");
        }
	}

	
	/* 
	 * Get string id of the transaction that would be when the Transaction
	 * is IN active state.
	 *
	 *This transaction "name" will be the same id which is returned in
	 * the TransactionInfo information if Tx is already in Active State.
	 * If the Transaction is in IDLE state, Transaction ID is 
	 * incremented when getActiveStateTxIdString() on raw transaction is called,
	 * instead of the Tx ID being incremented when Transaction gets into
	 * active state. The reason for incrementing the Tx ID earlier than when Tx
	 * is actually goes into active state is some debug statement cases like 
	 * log statement text. SQL  statements are wriited  to log before they are
	 * actully executed; In such cases we would like to display the actual TX ID on which 
	 * locks are acquired when the statement is executed.
	 * @return The a string which identifies the transaction.  
	 */
	public String getActiveStateTxIdString()
	{
		if(!justCreated && state == IDLE)
		{
			// TransactionTable needs this
			xactFactory.setNewTransactionId(myId, this);
			//mark as if this tx is just created , so that setActiveState()
			//does not increment the transaction id number.
			justCreated = true;
		}
		
		return toString();
	}


	/* package */
	String getState()
	{
		int localState ;

		// synchronized(this) -- int assignment, implicit synchronization
		// due to atomic action
		{
			localState = state;
		}

		switch (localState)
		{
		case CLOSED:
					return "CLOSED";
		case IDLE:
					return "IDLE";
		case ACTIVE:
		case UPDATE:
					return "ACTIVE";

		case PREPARED:
					return "PREPARED";
		}
		return null;
	}

    public String getTransName()
    {
        return transName;
    }

    public void setTransName(String name)
    {
        transName = name;
    }



	/**	
		Is the transaction in rollforward recovery
	*/
	public boolean inRollForwardRecovery()
	{
		return logFactory.inRFR();
	}


	/**	
		perform a  checkpoint during rollforward recovery
	*/
	public void checkpointInRollForwardRecovery(LogInstant cinstant,
												long redoLWM) 
		throws StandardException
	{
		logFactory.checkpointInRFR(cinstant, redoLWM, dataFactory);
	}

}

class LockCount {
	int count;
}


