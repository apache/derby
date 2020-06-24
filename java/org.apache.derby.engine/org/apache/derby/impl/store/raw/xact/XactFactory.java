/*

   Derby - Class org.apache.derby.impl.store.raw.xact.XactFactory

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

import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.util.InterruptStatus;

import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.AccessController;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class XactFactory implements TransactionFactory, ModuleControl, ModuleSupportable
{
	protected static final String USER_CONTEXT_ID        = "UserTransaction";
	protected static final String NESTED_READONLY_USER_CONTEXT_ID = 
        "NestedRawReadOnlyUserTransaction";
	protected static final String NESTED_UPDATE_USER_CONTEXT_ID = 
        "NestedRawUpdateUserTransaction";
	protected static final String INTERNAL_CONTEXT_ID    = "InternalTransaction";
	protected static final String NTT_CONTEXT_ID         = "NestedTransaction";

 	/*
	** Fields
	*/

	protected DaemonService rawStoreDaemon;

	private   UUIDFactory           uuidFactory;
	protected ContextService		contextFactory;
	protected LockFactory           lockFactory;
	protected LogFactory            logFactory;
	protected DataFactory           dataFactory;
	protected DataValueFactory      dataValueFactory;
	protected RawStoreFactory       rawStoreFactory;

	public TransactionTable ttab;
    /** The id of the next transaction to be started. */
    private final AtomicLong tranId = new AtomicLong();
	private LockingPolicy[][] lockingPolicies = new LockingPolicy[3][6];

	private boolean inCreateNoLog = false;	// creating database, no logging

    private /* XAResourceManager */ Object xa_resource;

	private Object   backupSemaphore = new Object();
	private long     backupBlockingOperations = 0;
	private boolean  inBackup = false;

	/*
	** Constructor
	*/

	public XactFactory() {
		super();
	}

	/*
	** Methods of ModuleControl
	*/
	public boolean canSupport(Properties startParams) {
		return true;
	}

	public void	boot(boolean create, Properties properties)
		throws StandardException
	{

		uuidFactory = getMonitor().getUUIDFactory();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        /*
        dataValueFactory =  (DataValueFactory)
            findServiceModule(
                this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                org.apache.derby.shared.common.reference.ClassName.DataValueFactory);
        */
            // if datafactory has not been booted yet, try now.  This can
            // happen in the unit tests.  Usually it is booted before store
            // booting is called.
            dataValueFactory = (DataValueFactory) 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                bootServiceModule(
                    create, 
                    this,
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
                    org.apache.derby.shared.common.reference.ClassName.DataValueFactory, 
                    properties);
		

		contextFactory = getContextService();
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		lockFactory = 
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
            (LockFactory) bootServiceModule(false, this,
				org.apache.derby.shared.common.reference.Module.LockFactory, properties);
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

		
        // adding entries to locking policy table which means we support that
        // level of concurrency.
		lockingPolicies[LockingPolicy.MODE_NONE]
                       [TransactionController.ISOLATION_NOLOCK] =
                            new NoLocking();

		lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_NOLOCK] =
                            new NoLocking();
		lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_READ_UNCOMMITTED] =
                            new RowLocking1(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_READ_COMMITTED] =
                            new RowLocking2(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
                            new RowLocking2nohold(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_REPEATABLE_READ] =
                            new RowLockingRR(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_RECORD]
                       [TransactionController.ISOLATION_SERIALIZABLE] =
                            new RowLocking3(lockFactory);

		lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_NOLOCK] =
                            new NoLocking();

        // note that current implementation of read uncommitted still gets
        // container and container intent locks to prevent concurrent ddl.  Thus
        // the read uncommitted containerlocking implementation is the same as
        // the read committed implementation.  Future customer requests may 
        // force us to change this - we will then have to figure out how to
        // handle a table being dropped while a read uncommitted scanner is
        // reading it - currently we just block that from happening.
		lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_READ_UNCOMMITTED] =
                            new ContainerLocking2(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_READ_COMMITTED] =
                            new ContainerLocking2(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
                            new ContainerLocking2(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_REPEATABLE_READ] =
                            new ContainerLocking3(lockFactory);
	    lockingPolicies[LockingPolicy.MODE_CONTAINER]
                       [TransactionController.ISOLATION_SERIALIZABLE] =
                            new ContainerLocking3(lockFactory);


		if (create)
		{
			ttab = new TransactionTable();

			String noLog =
				properties.getProperty(Property.CREATE_WITH_NO_LOG);

			inCreateNoLog = (noLog != null && Boolean.valueOf(noLog).booleanValue());

		}
	}

	public void	stop() {

		if (rawStoreDaemon != null)
			rawStoreDaemon.stop();

	}

	/*
	** Methods of TransactionFactory
	*/

	/**
		Get the LockFactory to use with this store.
	*/
	public LockFactory getLockFactory() {
		return lockFactory;
	}


	/**
		Database creation finished
		@exception StandardException standard Derby error policy
	*/
	public void createFinished() throws StandardException
	{
		if (!inCreateNoLog) 
        {
            throw StandardException.newException(SQLState.XACT_CREATE_NO_LOG);
        }

		// make sure there is no active update transaction
		if (ttab.hasActiveUpdateTransaction())
        {
            throw StandardException.newException(SQLState.XACT_CREATE_NO_LOG);
        }

		inCreateNoLog = false;
	}

    /**
     * Common work done to create local or global transactions.
     *
     * @param rsf    the raw store factory creating this xact.
     * @param parentTransaction parent transaction (if this is a nested user transaction)
     * @param cm     the current context manager to associate the xact with.
     * @param compatibilitySpace 
     *               if null, use the transaction being created, else if 
     *               non-null use this compatibilitySpace.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private Xact startCommonTransaction(
    RawStoreFactory     rsf, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
    Xact                    parentTransaction, 
    ContextManager      cm,
    boolean             readOnly,
    CompatibilitySpace  compatibilitySpace,
    String              xact_context_id,
    String              transName,
    boolean             excludeMe,
    boolean             flush_log_on_xact_end)
        throws StandardException
    {

		if (SanityManager.DEBUG)
		{
			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");

				SanityManager.ASSERT(
                    cm == contextFactory.getCurrentContextManager());
		}

		Xact xact = 
            new Xact(
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                this, parentTransaction, logFactory, dataFactory, dataValueFactory, 
                readOnly, compatibilitySpace, flush_log_on_xact_end);

        xact.setTransName(transName);
		pushTransactionContext(cm, xact_context_id, xact,
							   false /* abortAll */,
							   rsf,
							   excludeMe /* excludeMe during quiesce state */);
		return xact;
	}

	public RawTransaction startTransaction(
    RawStoreFactory rsf,
    ContextManager cm,
    String transName)
        throws StandardException
    {
        return(
            startCommonTransaction(
                rsf, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                null, 
                cm, 
                false,              // user xact always read/write 
                null, 
                USER_CONTEXT_ID, 
                transName, 
                true,               // user xact always excluded during quiesce
                true));             // user xact default flush on xact end
	}

	public RawTransaction startNestedReadOnlyUserTransaction(
    RawStoreFactory rsf,
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
    RawTransaction parentTransaction,
//IC see: https://issues.apache.org/jira/browse/DERBY-2328
    CompatibilitySpace compatibilitySpace,
    ContextManager  cm,
    String          transName)
        throws StandardException
    {
        return(
            startCommonTransaction(
                rsf, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                (Xact) parentTransaction, 
                cm, 
                true, 
                compatibilitySpace, 
                NESTED_READONLY_USER_CONTEXT_ID, 
                transName, 
                false,
                true));             // user readonly xact default flush on xact
                                    // end, should never have anything to flush.
	}

	public RawTransaction startNestedUpdateUserTransaction(
    RawStoreFactory rsf,
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
    RawTransaction parentTransaction,
    ContextManager  cm,
    String          transName,
    boolean         flush_log_on_xact_end)
        throws StandardException
    {
        return(
            startCommonTransaction(
                rsf, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                (Xact) parentTransaction, 
                cm, 
                false, 
                null, 
                NESTED_UPDATE_USER_CONTEXT_ID, 
                transName, 
                true,
                flush_log_on_xact_end));    // allow caller to choose default 
                                            // log log flushing on commit/abort
                                            // for internal operations used 
                                            // nested user update transaction.
	}

	public RawTransaction startGlobalTransaction(
    RawStoreFactory rsf,
    ContextManager  cm,
    int             format_id,
    byte[]          global_id,
    byte[]          branch_id)
        throws StandardException
    {
        GlobalXactId gid = new GlobalXactId(format_id, global_id, branch_id);

        if (ttab.findTransactionContextByGlobalId(gid) != null)
        {
            throw StandardException.newException(SQLState.STORE_XA_XAER_DUPID);
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6184
        Xact xact =
            startCommonTransaction(
                rsf, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                null, 
                cm, 
                false, 
                null, 
                USER_CONTEXT_ID, 
                AccessFactoryGlobals.USER_TRANS_NAME, 
                true,
                true);             // user xact default flush on xact end

        xact.setTransactionId(gid, xact.getId());

        return(xact);
	}



	public RawTransaction findUserTransaction(
    RawStoreFactory rsf,
    ContextManager  contextMgr,
    String transName)
		 throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                contextMgr == contextFactory.getCurrentContextManager(),
                "passed in context mgr not the same as current context mgr");

			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");
		}

		XactContext xc = (XactContext)contextMgr.getContext(USER_CONTEXT_ID);
		if (xc == null)
			return startTransaction(rsf, contextMgr, transName);
		else
			return xc.getTransaction();
 	}


	public RawTransaction startNestedTopTransaction(RawStoreFactory rsf, ContextManager cm)
        throws StandardException
    {

		if (SanityManager.DEBUG)
		{
			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");
		}

		Xact xact = 
            new Xact(
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                this, null, logFactory, dataFactory, dataValueFactory, 
                false, null, false);

		// hold latches etc. past commit in NTT
		xact.setPostComplete();
		pushTransactionContext(cm, NTT_CONTEXT_ID, xact, 
							   true /* abortAll */,
							   rsf, 
							   true /* excludeMe during quiesce state*/);
		return xact;
	}

	public RawTransaction startInternalTransaction(RawStoreFactory rsf, ContextManager cm) 
        throws StandardException 
    {
		if (SanityManager.DEBUG)
		{
			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");
		}


//IC see: https://issues.apache.org/jira/browse/DERBY-2537
		Xact xact = 
            new InternalXact(this, logFactory, dataFactory, dataValueFactory);

		pushTransactionContext(cm, INTERNAL_CONTEXT_ID, xact, 
							   true /* abortAll*/,
							   rsf,
							   true /* excludeMe during quiesce state */);
		return xact;
	}

	/*
	 * the following TransactionFactory methods are to support recovery and
	 * should only be used by recovery!
	 */

	/**
		Find the TransactionTableEntry with the given ID and make the passed in
		transaction assume the identity and properties of that
		TransactionTableEntry.
		Used in recovery only.
	*/
	public boolean findTransaction(TransactionId id,  RawTransaction tran)
	{
		return ttab.findAndAssumeTransaction(id, tran);
	}


	/**
		Rollback all active transactions that has updated the raw store.
		Use the recovery Transaction that is passed in to do all the work.
		Used in recovery only.

		<P>
		Transactions are rolled back in the following order:
		<OL>
		<LI>internal transactions in reversed beginXact chronological order,
		<LI>all other transactions in reversed beginXact chronological order,
		</NL>

		@param recoveryTransaction use this transaction to do all the user 
                                   transaction work

		@exception StandardException any exception thrown during rollback
	*/
	public void rollbackAllTransactions(
    RawTransaction  recoveryTransaction,
    RawStoreFactory rsf) 
        throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");

			SanityManager.ASSERT(
                recoveryTransaction != null, "recovery transaction null");
		}

		int irbcount = 0;

		// First undo internal transactions if there is any
		if (ttab.hasRollbackFirstTransaction())
		{
			RawTransaction internalTransaction = startInternalTransaction(rsf,
				recoveryTransaction.getContextManager());

			// make this transaction be aware that it is being used by recovery
			internalTransaction.recoveryTransaction();

			if (SanityManager.DEBUG)
				SanityManager.ASSERT(
                    internalTransaction.handlesPostTerminationWork() == false,
                    "internal recovery xact handles post termination work");

			while(ttab.getMostRecentRollbackFirstTransaction(
                                                internalTransaction))
			{
				irbcount++;
				internalTransaction.abort();
			}

			internalTransaction.close();
		}

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
                ttab.hasRollbackFirstTransaction() == false,
                "cant rollback user xacts with existing active internal xacts");
		}

		int rbcount = 0;

		// recoveryTransacion assumes the identity of the most recent xact
		while(ttab.getMostRecentTransactionForRollback(recoveryTransaction))
		{
			if (SanityManager.DEBUG)
            {
				SanityManager.ASSERT(
                    recoveryTransaction.handlesPostTerminationWork() == false,
                    "recovery transaction handles post termination work");
            }

			rbcount++;
			recoveryTransaction.abort();
		}

		if (SanityManager.DEBUG)
		{
			if (rbcount > 0 || irbcount > 0)
			{
				// RESOLVE: put this in the log trace
				//	System.out.println(
                //	    "Recovery rolled back " + irbcount + 
                //	    " internal transactions,"
				//			+ rbcount + " user transactions");
			}
		}

	}


	/**
        Run through all prepared transactions known to this factory 
        and restore their state such that they remain after recovery, and
        can be found and handled by a XA transaction manager.  This includes
        creating a context manager for each, pushing a xact context, and
        reclaiming update locks on all data changed by the transaction.

        Expected to be called just after the redo and undo recovery loops, 
        where the transaction table should be empty except for prepared
        xacts.

		Used only in recovery.

		@exception StandardException Derby Standard Error policy
	*/
	public void handlePreparedXacts(
    RawStoreFactory rsf)
        throws StandardException
	{
		if (SanityManager.DEBUG)
		{

			if (rawStoreFactory != null)
				SanityManager.ASSERT(
                    rawStoreFactory == rsf, "raw store factory different");
		}

        int prepared_count = 0;

		if (ttab.hasPreparedRecoveredXact())
		{
            // if there any prepared xacts 

            // At this point recovery has used one context and one transaction
            // to deal with all transactions.  Prepared transactions are to
            // be left in the transaction table, but the must have real and
            // separate CM's and transactions associated with them.

            // save old context.  Errors may go to funky contexts (the new
            // context we created to bring the prepared transaction into the
            // real world after recovery) after we switch contexts, but any 
            // error we get at this point is going to shut down the db.

            while (true)
            {
                // allocate new context and associate new xact with it.
                ContextManager cm      = contextFactory.newContextManager();
                contextFactory.setCurrentContextManager(cm);

				try {
                RawTransaction rawtran = 
                    startTransaction(
                        rawStoreFactory, cm, 
                        AccessFactoryGlobals.USER_TRANS_NAME);

                if (ttab.getMostRecentPreparedRecoveredXact(rawtran))
                {
                    // found a prepared xact.  The reprepare() call will 
                    // accumulate locks, and change the transaction table entry
                    // to not be "in-recovery" so that it won't show up again.
                    rawtran.reprepare();

                    if (SanityManager.DEBUG)
                        prepared_count++;
                }
                else
                {
                    // get rid of last transaction allocated.
                    rawtran.destroy();
                    break;
                }
				}
				finally
				{
					 contextFactory.resetCurrentContextManager(cm);
				}
            }

		}

		if (SanityManager.DEBUG)
		{
            // RESOLVE - need to only do this under a debug flag.
            // SanityManager.DEBUG_PRINT("",
            // "Recovery re-prepared " + prepared_count + " xa transactions.");
		}
	}


	/**
		Get the earliest log instant that is still active, ie, the first log
		record logged by the earliest transaction that is still active.
		<BR>
		The logging system must guarentee that the transaction table is
		populated in the order transactions are started.
		Used in recovery only.
	*/

	public LogInstant firstUpdateInstant()
	{
		return ttab.getFirstLogInstant();
	}

	/*
	** Methods of Corruptable
	*/

	/**
		Really this is just a convience routine for callers that might not
		have access to a log factory.
	*/
	public StandardException markCorrupt(StandardException originalError) {
		logFactory.markCorrupt(originalError);
		return originalError;
	}

	/*
	**		Implementation specific methods.
	*/

	public void setNewTransactionId(TransactionId oldxid, Xact t)
	{
		boolean excludeMe = true; // by default

		if (oldxid != null)
			excludeMe = remove(oldxid);

        XactId xid = new XactId(tranId.getAndIncrement());
//IC see: https://issues.apache.org/jira/browse/DERBY-4478

		t.setTransactionId(t.getGlobalId(), xid);

		// RESOLVE: How does a real global xact id get set?

		// If we got rid of the oldxid, that means this transaction object has
		// merely committed and starting the next transaction with the same
		// xact object.  In that case, the transaction context will remain the
		// same and won't be pushed.  We need to add this transaction with the
		// new id back into the transaction table.  If we did not get rid of
		// the old oldxid, that means this is a brand new transaction being
		// created.  The pushTransactionContext call will add it to the
		// transaction table with the appropriate flags
		if (oldxid != null)
			add(t, excludeMe);
	}

	/**
	**	Set the shortTranId, this is called by the log factory after recovery
	*/
	public void resetTranId()
	{
		XactId xid = (XactId)ttab.largestUpdateXactId();
//IC see: https://issues.apache.org/jira/browse/DERBY-4478
        long highestId = (xid == null) ? 0L : xid.getId();
        tranId.set(highestId + 1);
	}


	/**
		Create a new RawTransaction, a context for it and push the context
		onto the current context manager.  Then add the transacion to the
		transaction table.

		@param contextName the name of the transaction context
		@param xact the Transaction object
		@param abortAll if true, then any error will abort the whole
		transaction.  Otherwise, let XactContext.cleanupOnError decide what to
		do
		@param rsf the raw store factory
		@param excludeMe during systeme quiesce, i.e., this transaction should
		not be allowed to be active during a quiesce state.


		@exception StandardException Standard Derby error policy

	*/
	protected void pushTransactionContext(ContextManager cm, String contextName, 
										  Xact xact,
										  boolean abortAll, 
										  RawStoreFactory rsf,
										  boolean excludeMe)
		 throws StandardException 
	{
		if (cm.getContext(contextName) != null)	
        {
            throw StandardException.newException(
                    SQLState.XACT_TRANSACTION_ACTIVE);
        }
		
		XactContext xc = new XactContext(cm, contextName, xact, abortAll, rsf);

		// this transaction is now added to the transaction table.
		// This will cause an idle transaction to take on an identity, which is
		// unfortunate.  The reason why we have to add the transaction to the
		// table right now is because the transaction table is used to bring
		// system  to quisce state to  regulate who can go active during quiesce
		// state, and if we add the transaction
		// when it goes active, then there is a window where this transaction
		// can sneak in.  The transaction table itself does not keep track of
		// whether transactions can be started or not because quiesce related
		// transactions can start after all other user
		// transactions are excluded.  
		// RESOLVE: need to put more thought on the overall requirement and
		// design of the transaction table that satisfies the need of all the
		// clients, namely: checkpoint, recovery, quiesce mode, transaction table.

		add(xact, excludeMe);

	}

	/**
		Add a transaction to the list of transactions that has updated
		the raw store.  
		<P>
		This is called underneath the BeginXact log operation's doMe method.
		The logging system must guarentee that transactions are added in the
		true order they are started, as defined by the order of beginXact log
		record in the log.
	*/
	protected void addUpdateTransaction(
    TransactionId   id, 
    RawTransaction  t, 
    int             transactionStatus)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                id != null, "addding update transaction with null id");

		ttab.addUpdateTransaction(id, t, transactionStatus);
	}

	/**
		Remove a transaction from the list of transactions that has updated the
		raw store.
	*/
	protected void removeUpdateTransaction(TransactionId id)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                id != null, "remove update transaction with null id");

		ttab.removeUpdateTransaction(id);
	} 

	/**
        Change state of transaction to prepared.  Used by recovery to update
        the transaction table entry to prepared state.
	*/
	protected void prepareTransaction(TransactionId id)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
                id != null, "prepare transaction with null id");

		ttab.prepareTransaction(id);
	} 

	/**
		Submit this post commit work to the post commit daemon
	*/
	public boolean submitPostCommitWork(Serviceable work)
	{
		if (rawStoreDaemon != null)
			return rawStoreDaemon.enqueue(work, work.serviceASAP());
		return false;
	}

	public void setRawStoreFactory(RawStoreFactory rsf) throws StandardException 
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(rsf != null, "rawStoreFactory == null");
		}

		rawStoreFactory = rsf;

		// no need to remember raw store factory, 
		// just remember which daemon to use
		rawStoreDaemon = rsf.getDaemon();

		// now its ok to look for the log and data factory
		// log factory is booted by the data factory
		logFactory = (LogFactory) findServiceModule(this, rsf.getLogFactoryModule());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		// data factory is booted by the raw store implementation
		dataFactory = (DataFactory) findServiceModule(this, rsf.getDataFactoryModule());
	}

	/**
		Returns true if there is no in flight updating tranasaction.
		Caller must be aware that if there is no other mechanism to stop
		transactions from starting and ending, then this information is
		outdated as soon as it is reported.

		Only call this function in special times - e.g, during recovery
	*/
	public boolean noActiveUpdateTransaction()
	{
		return (ttab.hasActiveUpdateTransaction() == false);
	}


    /**
     * Check if there are any prepared transanctions in the 
     * transaction table. 
     *
     * Caller must be aware that if there is no other mechanism to stop
     * transactions from starting and ending, then this information is
     * outdated as soon as it is reported.
     *
     * @return     <tt>true</tt> if there are prepared 
     *              transactions in the transaction table,
     *              <tt>false</tt> otherwise.
     */
	public boolean hasPreparedXact()
	{
		return (ttab.hasPreparedXact());
	}



	/**
		remove the transaction Id an return false iff the transaction is found
		in the table and it doesn't need exclusion from quiesce state
	 */
	protected boolean remove(TransactionId xactId)
	{
		return ttab.remove(xactId);
	}

	protected void add(Xact xact, boolean excludeMe)
	{
		ttab.add(xact, excludeMe);
	}


	/**
		Make a new UUID for whomever that wants it
	*/
	public UUID makeNewUUID()
	{
		return uuidFactory.createUUID();
	}

	/**
		Get a locking policy for a transaction.
	*/
	final LockingPolicy getLockingPolicy(
    int     mode, 
    int     isolation, 
    boolean stricterOk)
    {

		if (mode == LockingPolicy.MODE_NONE)
			isolation = TransactionController.ISOLATION_NOLOCK;

		LockingPolicy policy = lockingPolicies[mode][isolation];

		if ((policy != null) || (!stricterOk))
			return policy;

		for (mode++; mode <= LockingPolicy.MODE_CONTAINER; mode++) 
        {
			for (int i = isolation; 
                 i <= TransactionController.ISOLATION_SERIALIZABLE; 
                 i++) 
            {
				policy = lockingPolicies[mode][i];
				if (policy != null)
					return policy;
			}
		}

		return null;
	}

	/**
		Return the transaction table to be logged with the checkpoint operation
	 */
	public Formatable getTransactionTable()
	{
		return ttab;
	}

	/**
		Use this transaction table, which is gotten from a checkpoint
		operation.  Use ONLY during recovery.
	 */
	public void useTransactionTable(Formatable transactionTable) 
		 throws StandardException 
	{
		if (ttab != null && transactionTable != null)
        {
            throw StandardException.newException(
                    SQLState.XACT_TRANSACTION_TABLE_IN_USE);
        }

		if (ttab == null)
		{
			if (transactionTable == null)
				ttab = new TransactionTable();
			else
			{
				if (SanityManager.DEBUG)
				{
					if ((transactionTable instanceof TransactionTable) ==
						false)
					{
						SanityManager.THROWASSERT(
							"using transaction table which is of class " + 
							transactionTable.getClass().getName());
					}
				}
				ttab = (TransactionTable)transactionTable;
			}
		}
		// else transactionTable must be null, if we already have a transaction
		// table, no need to do anything
	}

	public TransactionInfo[] getTransactionInfo()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(ttab != null, "transaction table is null");
		return ttab.getTransactionInfo();
	}


	/**
	 * @return false, if the Database creation finished
	 */
	public boolean inDatabaseCreation()
	{
		return inCreateNoLog;
	}
	
	/**
	 * Return the module providing XAresource interface to the transaction 
     * table. 
     *
	 * @exception StandardException Standard Derby exception policy.
	 */
	public /* XAResourceManager */ Object getXAResourceManager()
        throws StandardException
    {
        if (xa_resource == null)
            xa_resource = new XactXAResourceManager(rawStoreFactory, ttab);

        return(xa_resource);
    }


    /**
     * Block the online backup. Backup needs to be blocked while 
     * executing any unlogged operations or any opearation that 
     * prevents from  making a consistent backup.
     * 
     * @param wait if <tt>true</tt>, waits until the backup 
     *             is blocked. 
     * @return     <tt>true</tt> if backup is blocked.
     *			   <tt>false</tt> otherwise.
     */
	protected boolean blockBackup(boolean wait)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-239
		synchronized(backupSemaphore) {
            // do not allow backup blocking operations, if online backup is
            // is in progress.
			if (inBackup) 
            {
                if(wait) {
                    while(inBackup) {
                        try {
                            backupSemaphore.wait();
                        } catch (InterruptedException ie) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                            InterruptStatus.setInterrupted();
                        }
                    }
                }else {
                    return false;
                }
			}

            // not in online backup, allow backup blocking operations
            backupBlockingOperations++;
            return true;
		}
	}


	/**
     * Unblock the backup, a backup blocking operation finished. 
	 */
	protected void unblockBackup()
	{
		synchronized(backupSemaphore) {
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(backupBlockingOperations > 0, 
                    "no backup blocking opeations in progress"); 
			
			backupBlockingOperations--;

			if (inBackup) {
				// wake up the online backupthread
				backupSemaphore.notifyAll(); 
			}
		}
	}

	/**
	 * Checks if there are any backup blocking operations in progress and 
	 * prevents new ones from starting until the backup is finished. 
	 * If backup blocking operations are in progress and  <code> wait </code>
	 * parameter value is <tt>true</tt>, then it will wait for the current 
	 * backup blocking operations to finish. 
	 * 
	 * A Consistent backup can not be made if there are any backup 
	 * blocking operations (like unlogged operations) are in progress
	 *
	 * @param wait if <tt>true</tt>, waits for the current backup blocking 
	 *             operation in progress to finish.
	 * @return     <tt>true</tt> if no backup blocking operations are in 
     *             progress
	 *             <tt>false</tt> otherwise.
     * @exception RuntimeException if runtime exception occurs, in which case
     *             other threads blocked on backupSemaphore are notified
	 */
	public boolean blockBackupBlockingOperations(boolean wait) 
	{
		synchronized(backupSemaphore) {
			if (wait) {
				// set the inBackup state to true first to stop new backup
				// blocking operation from starting.
				inBackup= true;
				try	{
					// wait for backup blocking operation in progress to finish
					while(backupBlockingOperations > 0)
					{
						try	{
							backupSemaphore.wait();
						}
						catch (InterruptedException ie) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4741
                            InterruptStatus.setInterrupted();
						}
					}
				}
				catch (RuntimeException rte) {
					// make sure we are not stuck in backup state if we
					// caught a run time exception and the calling thread may 
                    // not have a chance to clear the in backup state.
					inBackup= false;
					backupSemaphore.notifyAll();
					throw rte;		// rethrow run time exception
				}
			} else {
				// check if any backup blocking operations that are in  progress
				if (backupBlockingOperations == 0)
					inBackup = true;
			}
            
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-239
//IC see: https://issues.apache.org/jira/browse/DERBY-523
        if (SanityManager.DEBUG) {
            if (inBackup) {
                SanityManager.ASSERT(backupBlockingOperations == 0 ,
                                 "store is not in correct state for backup");
            }
        }

		return inBackup;
	}


	/**
	 * Backup completed. Allow backup blocking operations. 
	 */
	public void unblockBackupBlockingOperations()
	{
		synchronized(backupSemaphore) {
			inBackup = false;
			backupSemaphore.notifyAll();
		}
	}
	
    
    /**
     * Privileged lookup of the ContextService. Private so that user code
     * can't call this entry point.
     */
    private static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ContextService>()
             {
                 public ContextService run()
                 {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                     return ContextService.getFactory();
                 }
             }
             );
    }

    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
        (
         final boolean create, final Object serviceModule,
         final String factoryInterface, final Properties properties
         )
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

}
