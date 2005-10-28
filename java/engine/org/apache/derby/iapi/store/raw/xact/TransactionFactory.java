/*

   Derby - Class org.apache.derby.iapi.store.raw.xact.TransactionFactory

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

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.locks.LockFactory;

import org.apache.derby.iapi.store.access.TransactionInfo;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Corruptable;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

/**
	This module is intended to be used only within the RawStore. RawStore functionality
	is accessed only through the RawStoreFactory interface.
	The transaction manager is responsible for:

	<UL>
	<LI>Generating unique transaction identifiers.
	<LI>Keeping a list of all open transactions within the raw store.
	</UL>

	@see RawStoreFactory
	@see Transaction
*/

public interface TransactionFactory extends Corruptable {

	public static String MODULE =
        "org.apache.derby.iapi.store.raw.xact.TransactionFactory";

	/**
		Get the LockFactory to use with this store.
	*/
	public LockFactory getLockFactory();

	/*
	 * Return the module providing XAresource interface to the transaction
     * table.
     *
	 * @exception StandardException Standard cloudscape exception policy.
	 */
	public /* XAResourceManager */ Object getXAResourceManager()
        throws StandardException;

	/**
		Start a new transaction within the given raw store. This method will
        push a transaction context as described in
        RawStoreFactory.startTransaction

        @param contextMgr is the context manager to use.  It must be the current
                          context manager.
        @param transName is the transaction name. It will be displayed in the
            transactiontable VTI.

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startTransaction(
    RawStoreFactory rsf,
    ContextManager  contextMgr,
    String transName)
        throws StandardException;

	/**
		Start a new read only transaction within the given raw store. This 
        method will push a transaction context as described in
        RawStoreFactory.startNestedTransaction

		@param compatibilitySpace   compatibility space to use for locks.
        @param contextMgr           is the context manager to use.  It must be 
                                    the current context manager.
        @param transName            is the transaction name. It will be 
                                    displayed in the transactiontable VTI.

		@see RawStoreFactory#startNestedReadOnlyUserTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startNestedReadOnlyUserTransaction(
    RawStoreFactory rsf,
    Object          compatibilitySpace,
    ContextManager  contextMgr,
    String          transName)
        throws StandardException;

	/**
		Start a new update transaction within the given raw store. This method 
        will push a transaction context as described in
        RawStoreFactory.startNestedTransaction

        @param contextMgr           is the context manager to use.  It must be 
                                    the current context manager.
        @param transName            is the transaction name. It will be 
                                    displayed in the transactiontable VTI.

		@see RawStoreFactory#startNestedUpdateUserTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startNestedUpdateUserTransaction(
    RawStoreFactory rsf,
    ContextManager  contextMgr,
    String          transName)
        throws StandardException;


	/**
		Start a new transaction within the given raw store. This method will
        push a transaction context as described in
        RawStoreFactory.startTransaction

        @param format_id  the format id part of the Xid - ie. Xid.getFormatId().
        @param global_id  the global transaction identifier part of XID - ie.
                          Xid.getGlobalTransactionId().
        @param branch_id  The branch qualifier of the Xid - ie.
                          Xid.getBranchQaulifier()
        @param contextMgr is the context manager to use.  It must be the current
                          context manager.

		@see RawStoreFactory#startGlobalTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startGlobalTransaction(
    RawStoreFactory rsf,
    ContextManager  contextMgr, 
    int             format_id,
    byte[]          global_id,
    byte[]          branch_id)
        throws StandardException;

	/**
		Find a user transaction within the given raw store and the given
		contextMgr.  If no user transaction exist, then start one with name
        transName. This method will push a transaction context as described in
		RawStoreFactory.startTransaction

		@see RawStoreFactory#findUserTransaction
		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction findUserTransaction(
        RawStoreFactory rsf,
        ContextManager contextMgr,
        String transName) throws StandardException;

	/**
		Start a new nested top transaction within the given raw store. This
        method will push a transaction context as described in
        RawStoreFactory.startNestedTopTransaction

		@see RawStoreFactory#startTransaction(ContextManager, String)

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startNestedTopTransaction(RawStoreFactory rsf, ContextManager contextMgr) throws StandardException;


	/**
		Start a new internal transaction within the given raw store. This 
        method will push a transaction context as described in 
        RawStoreFactory.startInternalTransaction

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Cloudscape error policy.
	*/
	public RawTransaction startInternalTransaction(RawStoreFactory rsf, ContextManager contextMgr) throws StandardException;


	/**
		Find a transaction using a transactionId and make the passed in
		transaction assume the identity and properties of that transaction.
		Used in recovery only.
	*/
	public boolean findTransaction(TransactionId id, RawTransaction tran);


	/**
		Reset any resettable transaction Id 
		@exception StandardException Standard Cloudscape error policy.
	*/
	public void resetTranId() throws StandardException;

	/**
		The first log instant that belongs to a transaction that is still
		active in the raw store. This is the first log record of the longest
		running transaction at this moment. 
	*/
	public LogInstant firstUpdateInstant();

	/**
        Run through all prepared transactions known to this factory 
        and restore their state such that they remain after recovery, and
        can be found and handled by a XA transaction manager.  This includes
        creating a context manager for each, pushing a xact context, and
        reclaiming update locks on all data changed by the transaction.

		Used only in recovery.

		@exception StandardException Cloudscape Standard Error policy
	*/
	public void handlePreparedXacts(
    RawStoreFactory rsf)
		 throws StandardException;


	/**
		Rollback and close all transactions known to this factory using a
		passed in transaction.  Used only in recovery.

		@param recoveryTransaction the transaction used to rollback
		@exception StandardException Cloudscape Standard Error policy
	*/
	public void rollbackAllTransactions(RawTransaction recoveryTransaction, 
										RawStoreFactory rsf)
		 throws StandardException ;


	/**
		Submit a post commit work to the post commit daemon.
		The work is always added to the deamon, regardless of the
		state it returns.

		@return true if the daemon indicates it is being overloaded,
		false it's happy.

		<MT> must be MT-safe
	*/
	public boolean submitPostCommitWork(Serviceable work);

	/**
		make Transaction factory aware of which raw store factory it belongs to
	*/
	public void setRawStoreFactory(RawStoreFactory rsf) throws StandardException;

	/**
		Returns true if the transaction factory has no active updating 
        transaction
	*/
	public boolean noActiveUpdateTransaction();

	/**
		Database creation finished

		@exception StandardException Standard cloudscape exception policy.
	*/
	public void createFinished() throws StandardException;

	/**
		Return the transaction table so it can get logged with the checkpoint
		log record.
	 */
	public Formatable getTransactionTable();

	/**
		Use this transaction table, which is gotten from a checkpoint
		operation.  Use ONLY during recovery.

		@exception StandardException Standard cloudscape exception policy.
	 */
	public void useTransactionTable(Formatable transactionTable) 
		 throws StandardException; 

	/**
	  @see org.apache.derby.iapi.store.access.AccessFactory#getTransactionInfo
	 */
	public TransactionInfo[] getTransactionInfo();
}
