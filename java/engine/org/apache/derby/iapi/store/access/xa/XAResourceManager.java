/*

   Derby - Class org.apache.derby.iapi.store.access.xa.XAResourceManager

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.access.xa;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException; 

import javax.transaction.xa.Xid;

/**

This interface allows access to commit,prepare,abort global transactions
as part of a two phase commit protocol.  These interfaces have been chosen
to be exact implementations required to implement the XAResource interfaces
as part of the JTA standard extension.
<P>
It is expected that the following interfaces are only used during the 
recovery portion of 2 phase commit, when the transaction manager is
cleaning up after a runtime crash - it is expected that no current context
managers exist for the Xid's being operated on.  The "online" two phase commit
protocol will be implemented by calls directly on a TransactionController.
<P>
The XAResource interface is a Java mapping of the industry standard XA resource
manager interface.  Please refer to: X/Open CAE Specification - Distributed 
Transaction Processing: The XA Specification, X/Open Document No. XO/CAE/91/300
or ISBN 1 872630 24 3.
<P>

**/

public interface XAResourceManager
{
    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * This method is called to commit the global transaction specified by xid.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param cm       The ContextManager returned from the find() call.
     * @param xid      A global transaction identifier.
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void commit(
    ContextManager  cm,
    Xid             xid,
    boolean         onePhase)
		throws StandardException;

    /**
     * Find the given Xid in the transaction table.
     * <p>
     * This routine is used to find a in-doubt transaction from the list
     * of Xid's returned from the recover() routine.  
     * <p>
     * In the current implementation it is up to the calling routine
     * to make the returned ContextManager the "current" ContextManager
     * before calls to commit,abort, or forget.  The caller is responsible
     * for error handling, ie. calling cleanupOnError() on the correct
     * ContextManager.
     * <p>
     * If the Xid is not in the system, "null" is returned.
     * RESOLVE - find out from sku if she wants a exception instead?
     * <p>
     *
     * @param xid      A global transaction identifier.
     **/
    public ContextManager find(
    Xid     xid);

    /**
     * This method is called to remove the given transaction 
     * from the transaction table/log.
     * <p>
     * Used to let the store remove all record from log and transaction
     * table of the given transaction.  This should only be used to 
     * clean up heuristically completed transactions, otherwise commit or
     * abort should be used to act on other transactions.
     * <p>
     * If forget() is called on a transaction which has not be heuristically
     * completed then it will throw an exception:
     * SQLState.STORE_XA_PROTOCOL_VIOLATION.
     *
     * @param cm       The ContextManager returned from the find() call.
     * @param xid      A global transaction identifier.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     **/
    public void forget(
    ContextManager  cm,
    Xid             xid)
		throws StandardException;

    /**
     * This method is called to obtain a list of prepared transactions.
     * <p>
     * This call returns a complete list of global transactions which are 
     * either prepared or heuristically complete.
     * <p>
     * The XAResource interface expects a scan type interface, but our
     * implementation only returns a complete list of transactions.  So to
     * simulate the scan the following state is maintained.  If TMSTARTSCAN
     * is specified the complete list is returned.  If recover is called with
     * TMNOFLAGS is ever called a 0 length array is returned.  
     *
	 * @return Return a array with 0 or more Xid's which are currently in
     *         prepared or heuristically completed state.  If an error occurs
     *         during the operation, an appropriate error is thrown.
     *
     * @param flags    combination of the following flags 
     *                 XAResource.{TMSTARTRSCAN,TMENDRSCAN,TMNOFLAGS}.  
     *                 TMNOFLAGS must be used when no other flags are used.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Xid[] recover(int flags)
        throws StandardException;

    /**
     * rollback the transaction identified by Xid.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     *
     * @param cm       The ContextManager returned from the find() call.
     * @param xid      A global transaction identifier.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void rollback(
    ContextManager  cm,
    Xid             xid)
        throws StandardException;
}
