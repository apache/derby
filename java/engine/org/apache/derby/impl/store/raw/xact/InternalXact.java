/*

   Derby - Class org.apache.derby.impl.store.raw.xact.InternalXact

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

import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.store.raw.log.LogFactory;

import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.ObjectInput;

/**

	@see Xact

*/
public class InternalXact extends Xact  
{

	/*
	** Constructor
	*/

	protected InternalXact(
    XactFactory     xactFactory, 
    LogFactory      logFactory, 
    DataFactory     dataFactory) 
    {
		super(xactFactory, logFactory, dataFactory, false, null);

		// always want to hold latches & containers open past the commit/abort
		setPostComplete();
	}

	/*
	** Methods of Transaction
	*/

  
	/**
		Savepoints are not supported in internal transactions.

	    @exception StandardException  A transaction exception is thrown to 
                                      disallow savepoints.

		@see Transaction#setSavePoint
	*/
	public int setSavePoint(String name, Object kindOfSavepoint) 
        throws StandardException 
    {
		throw StandardException.newException(
                SQLState.XACT_NOT_SUPPORTED_IN_INTERNAL_XACT);
	}


	/*
	** Methods of RawTransaction
	*/
	/**
		Internal transactions don't allow logical operations.

		@exception StandardException A transaction exception is thrown to 
                                     disallow logical operations.

		@see org.apache.derby.iapi.store.raw.xact.RawTransaction#recoveryRollbackFirst
	*/
	
	 public void checkLogicalOperationOK() 
         throws StandardException 
     {
		throw StandardException.newException(
                SQLState.XACT_NOT_SUPPORTED_IN_INTERNAL_XACT);
	 }

	/**
		Yes, we do want to be rolled back first in recovery.

		@see org.apache.derby.iapi.store.raw.xact.RawTransaction#recoveryRollbackFirst
	*/
	public boolean recoveryRollbackFirst()
    {
		return true;
	}

	/*
	**	Implementation specific methods
	*/

	/**
	 * @param commitOrAbort to commit or abort
	 *
	 * @exception StandardException on error
	 */
	protected void doComplete(Integer commitOrAbort) 
        throws StandardException 
    {

		// release our latches on an abort
		// keep everything on a commit
		if (commitOrAbort.equals(ABORT))
			super.doComplete(commitOrAbort);
	}

	protected void setIdleState() 
    {

		super.setIdleState();

		// Quiesce mode never denies an internal transaction from going active, don't
		// have to worry about that
		if (countObservers() != 0)
		{
			try
			{
				super.setActiveState();
			}
			catch (StandardException se)
			{
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("unexpected exception: " + se);
			}
		}
	}
}
