/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.error.ExceptionSeverity;
public class RAMTransactionContext extends ContextImpl
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	The transaction this context is managing.
	**/
	protected RAMTransaction transaction;

	/**
	   true if any exception causes this transaction to be destroyed
	**/
	private boolean abortAll;

	/*
	** Context methods (most are implemented by super-class).
	*/

	/**
	Handle cleanup processing for this context. The resources
	associated with a transaction are the open controllers.
	Cleanup involves closing them at the appropriate time.
	Rollback of the underlying transaction is handled by the
	raw store.
	**/
	public void cleanupOnError(Throwable error)
        throws StandardException
	{
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(getContextManager() != null);

		boolean destroy = false;

		if (abortAll == false && (error instanceof StandardException))
		{
			StandardException se = (StandardException) error;

			// If the severity is lower than a transaction error then do nothing.
			if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
				return;

			// If the session is going to disappear then we want to destroy this
			// transaction, not just abort it.
			if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
				destroy = true;
		}
		else
		{
			// abortAll is true or some java* error, throw away the
			// transaction. 
			destroy = true;
		}

        if (transaction != null)
        {
            try
            {
                transaction.invalidateConglomerateCache();
            }
            catch (StandardException se)
            {
                // RESOLVE - what to do in error case.
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT(
                        "got error while invalidating cache.");
            }

            transaction.closeControllers(true /* close held controllers */ );
        }

        if (destroy)
        {
            transaction = null;
            popMe();
        }

	}

	/*
	** Methods of RAMTransactionContext
	*/

	// this constructor is called with the transaction
	// controller to be saved when the context
	// is created (when the first statement comes in, likely).
	public RAMTransactionContext(
    ContextManager  cm,
    String          context_id,
    RAMTransaction  theTransaction, 
    boolean         abortAll)
		throws StandardException
	{
		super(cm, context_id);

		this.abortAll = abortAll;
		transaction = theTransaction;
		transaction.setContext(this);
	}

	/* package */ RAMTransaction getTransaction()
	{
		return transaction;
	}

	/* package */ void setTransaction(
    RAMTransaction  transaction)
	{
		this.transaction = transaction;
	}
}


