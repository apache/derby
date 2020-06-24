/*

   Derby - Class org.apache.derby.impl.store.raw.xact.XactXAResourceManager

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.store.access.xa.XAResourceManager;
import org.apache.derby.iapi.store.access.xa.XAXactId;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;

import java.util.ArrayList;

import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;

/**

The XactXAResourceManager implements the Access XAResource interface, which
provides offline control over two phase commit transactions.  It is expected
to be used by TM's (transaction manager's), to recover if systems fail while
transactions are still in-doubt (prepared).
<P>
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

@see org.apache.derby.iapi.store.access.xa.XAResourceManager

**/

public class XactXAResourceManager implements XAResourceManager
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private TransactionTable    transaction_table;
    private RawStoreFactory     rsf;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public XactXAResourceManager(
    RawStoreFactory     rsf,
    TransactionTable    tt)
    {
        this.rsf               = rsf;
        this.transaction_table = tt;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods implementing XAResourceManager interface
     **************************************************************************
     */

    /**
     * This method is called to commit the global transaction specified by xid.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
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
		throws StandardException
    {
        Transaction rawtran = 
            rsf.findUserTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);

        // This may happen if somehow the transaction was committed between
        // the find() call and now.
        if (rawtran == null)
        {
            throw StandardException.newException(
                    SQLState.STORE_XA_PROTOCOL_VIOLATION);
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(rawtran != null);

            SanityManager.ASSERT(
                (new GlobalXactId(
                    xid.getFormatId(),
                    xid.getGlobalTransactionId(),
                    xid.getBranchQualifier())).equals(rawtran.getGlobalId()));
        }

        rawtran.xa_commit(onePhase);
    }

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
     *
     **/
    public ContextManager find(
    Xid     xid)
    {
        return(
            transaction_table.findTransactionContextByGlobalId(
                new GlobalXactId(
                    xid.getFormatId(),
                    xid.getGlobalTransactionId(),
                    xid.getBranchQualifier())));
    }

    /**
     * This method is called to remove the given transaction 
     * from the transaction table/log.
     * <p>
     * Used to let the store remove all record from log and transaction
     * table of the given transaction.  This should only be used to 
     * clean up heuristically completed transactions, otherwise commit or
     * abort should be used to act on other transactions.
     * <p>
     *
     * @param cm       The ContextManager returned from the find() call.
     * @param xid      A global transaction identifier.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void forget(
    ContextManager  cm,
    Xid             xid)
		throws StandardException
    {
        Transaction rawtran = 
            rsf.findUserTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                new GlobalXactId(
                    xid.getFormatId(),
                    xid.getGlobalTransactionId(),
                    xid.getBranchQualifier()).equals(rawtran.getGlobalId()));
        }

        // forget should only be called on heuristically completed xacts, which
        // should not exist in our system.
        throw StandardException.newException(
                SQLState.STORE_XA_PROTOCOL_VIOLATION);
    }


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
        throws StandardException
    {
        XAXactId[] ret_xid_list;

        if ((flags & XAResource.TMSTARTRSCAN) != 0)
        {
            final ArrayList<XAXactId> xid_list = new ArrayList<XAXactId>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

            // Create a visitor that adds each of the prepared transactions
            // to xid_list.
//IC see: https://issues.apache.org/jira/browse/DERBY-3092
            final TransactionTable.EntryVisitor visitor =
                    new TransactionTable.EntryVisitor() {
                public boolean visit(TransactionTableEntry entry) {
                    Xact xact = entry.getXact();
                    if (xact.isPrepared())
                    {
                        GlobalXactId xa_id = (GlobalXactId) xact.getGlobalId();
//IC see: https://issues.apache.org/jira/browse/DERBY-6184

                        xid_list.add(
                            new XAXactId(
                                xa_id.getFormat_Id(), 
                                xa_id.getGlobalTransactionId(), 
                                xa_id.getBranchQualifier()));
                    }
//IC see: https://issues.apache.org/jira/browse/DERBY-3092
                    return true; // scan the entire transaction table
                }
            };

            // Collect the prepared transactions.
            transaction_table.visitEntries(visitor);

            // Convert the list to an array suitable for being returned.
            ret_xid_list = new XAXactId[xid_list.size()];
            ret_xid_list = (XAXactId[]) xid_list.toArray(ret_xid_list);
        }
        else
        {
            ret_xid_list = new XAXactId[0];
        }

        return(ret_xid_list);
    }

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
        throws StandardException
    {
        Transaction rawtran = 
            rsf.findUserTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);

        // This may happen if somehow the transaction was committed between
        // the find() call and now.
        if (rawtran == null)
        {
            throw StandardException.newException(
                    SQLState.STORE_XA_PROTOCOL_VIOLATION);
        }

        if (SanityManager.DEBUG)
        {

            SanityManager.ASSERT(
                new GlobalXactId(
                    xid.getFormatId(),
                    xid.getGlobalTransactionId(),
                    xid.getBranchQualifier()).equals(rawtran.getGlobalId()));
        }

        rawtran.xa_rollback();
    }

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
