/*

   Derby - Class org.apache.derby.impl.store.raw.xact.XactContext

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

import org.apache.derby.shared.common.reference.SQLState;

// This is the recommended super-class for all contexts.
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.shared.common.error.ExceptionSeverity;

/**
The context associated with the transaction.

This object stores the context associated with the raw store transaction
on the stack.  It stores info about the transaction opened within a 
context manager (ie. typically a single user) for a single RawStoreFactory.

**/

final class XactContext extends ContextImpl {
//IC see: https://issues.apache.org/jira/browse/DERBY-467

	private		RawTransaction	xact;
	private     RawStoreFactory factory;
	private		boolean   abortAll; // true if any exception causes this transaction to be aborted.

	XactContext(ContextManager cm, String name, Xact xact, boolean abortAll, RawStoreFactory factory) {
		super(cm, name);

		this.xact = xact;
		this.abortAll = abortAll;
		this.factory = factory;
		xact.xc = this;	// double link between transaction and myself
	}


	/*
	** Context methods (most are implemented by super-class)
	*/


	/**
		@exception StandardException Standard Derby error policy
	*/
	public void cleanupOnError(Throwable error) throws StandardException {

        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT(getContextManager() != null);
    	}

		boolean throwAway = false;

		if (error instanceof StandardException) {
			StandardException se = (StandardException) error;

			if (abortAll) {
				// any error aborts an internal/nested xact and its transaction

				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					throw StandardException.newException(
                        SQLState.XACT_INTERNAL_TRANSACTION_EXCEPTION, error);
                }

				throwAway = true;


			} else {

				// If the severity is lower than a transaction error then do nothing.
				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					return;
                }
                 

				// If the session is going to disappear then we want to close this
				// transaction, not just abort it.
				if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
					throwAway = true;
			}
		} else {
			// some java* error, throw away the transaction.
			throwAway = true;
		}

		try {

			if (xact != null) {
				// abort the transaction
				xact.abort();
			}

		} catch (StandardException se) {
			// if we get an error during abort then shut the system down
			throwAway = true;

			// if the system was being shut down anyway, do nothing
			if ((se.getSeverity() <= ExceptionSeverity.SESSION_SEVERITY) &&
				(se.getSeverity() >= ((StandardException) error).getSeverity())) {

				throw factory.markCorrupt(
                    StandardException.newException(
                        SQLState.XACT_ABORT_EXCEPTION, se));
			}

		} finally {

			if (throwAway) {
				// xact close will pop this context out of the context
				// stack 
				xact.close();
				xact = null;
			}
		}

	}

//IC see: https://issues.apache.org/jira/browse/DERBY-467
	RawTransaction getTransaction() {
		return xact;
	}

	RawStoreFactory getFactory() {
		return factory;
	}

	void substituteTransaction(Xact newTran)
	{
		// disengage old tran from this xact context
		Xact oldTran = (Xact)xact;
		if (oldTran.xc == this)
			oldTran.xc = null;

		// set up double link between new transaction and myself
		xact = newTran;
		((Xact)xact).xc = this;
	}

}
