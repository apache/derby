/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_TWC

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.store;

import org.apache.derby.iapi.store.raw.*;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;

/**
	Transaction with context, a utility class for tests to create
	multiple interleaving transactions.
*/
public class T_TWC
{
	protected Transaction tran;
	protected ContextManager cm;
	protected ContextService contextService;
	protected LockFactory lf;
	protected RawStoreFactory rawStore;

	public T_TWC(ContextService contextService,
					LockFactory lockFactory,
					RawStoreFactory rawStoreFactory)
	{
		this.contextService = contextService;
		this.lf = lockFactory;
		this.rawStore = rawStoreFactory;
		tran = null;
		cm = null;
	}

	public T_TWC startUserTransaction()
		 throws StandardException
	{
		cm = contextService.newContextManager();
		contextService.setCurrentContextManager(cm);
		try {
		tran = 
            rawStore.startTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(tran != null);
		checkNullLockCount();
		}
		finally {
			contextService.resetCurrentContextManager(cm);
		}
		return this;
	}

	public void checkNullLockCount()
	{
		switchTransactionContext();
		try {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(
				!lf.areLocksHeld(tran.getCompatibilitySpace()),
				"Transaction holds locks.");
		} finally {
			resetContext();
		}
	}

	public void setSavePoint(String sp, Object kindOfSavepoint)
		 throws StandardException
	{
		switchTransactionContext();
		try {
		tran.setSavePoint(sp, null);
		} finally {
			resetContext();
		}
	}

	public void rollbackToSavePoint(String sp, Object kindOfSavepoint)
		 throws StandardException
	{
		switchTransactionContext();
		try {
			tran.rollbackToSavePoint(sp, null);
		} finally {
			resetContext();
		}
	}

	public void switchTransactionContext()
	{
		contextService.setCurrentContextManager(cm);
	}
	public void resetContext() {
		contextService.resetCurrentContextManager(cm);
	}

	public void logAndDo(Loggable l)
		 throws StandardException
	{
		switchTransactionContext();
		try {
			tran.logAndDo(l);
		} finally {
			resetContext();
		}
	}

	public void commit()
		 throws StandardException
	{
		switchTransactionContext();
		try {
		tran.commit();
		} finally {
			resetContext();
		}
		checkNullLockCount();		
	}

	public void abort()
		 throws StandardException
	{
		switchTransactionContext();
		try {
		tran.abort();
		} finally {
			resetContext();
		}
		checkNullLockCount();		
	}


	public GlobalTransactionId getId()
		 throws StandardException
	{
		switchTransactionContext();
		try {
			return tran.getGlobalId();
		} finally {
			resetContext();
		}

	}
}
