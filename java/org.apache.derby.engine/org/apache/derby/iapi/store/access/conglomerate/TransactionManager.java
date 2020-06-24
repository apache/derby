/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.TransactionManager

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

package org.apache.derby.iapi.store.access.conglomerate;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.shared.common.error.StandardException;


/**

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
<p>
The TransactionManager interface provides methods on the transaction needed
by an access method implementer, but should not be visible to clients of a
TransactionController.
</p>
@see TransactionController

**/

public interface TransactionManager extends TransactionController
{

    /**
     * Constant used for the lock_level argument to openConglomerate() and 
     * openScan() calls.  Pass in MODE_NONE if you want no table or row locks.
     * This is currently only supported from within access.
     **/
	static final int MODE_NONE      = 5;

    /**
     * release lock immediately after getting lock.
     **/
    public static final int LOCK_INSTANT_DURATION   = 1;
    /**
     * hold lock until end of transaction.
     **/
    public static final int LOCK_COMMIT_DURATION    = 2;
    /**
     * Allow lock to be released manually prior to end transaction.
     **/
    public static final int LOCK_MANUAL_DURATION    = 3;

    /**
     * Add to the list of post commit work.
     * <p>
     * Add to the list of post commit work that may be processed after this
     * transaction commits.  If this transaction aborts, then the post commit
     * work list will be thrown away.  No post commit work will be taken out
     * on a rollback to save point.
     * <p>
     * This routine simply delegates the work to the Rawstore transaction.
     *
     * @param work  The post commit work to do.
     *
     **/
	public void addPostCommitWork(Serviceable work);

    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature               Non-null to throw an exception, null to 
     *                              return the state of the version match.
     *
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     *
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public boolean checkVersion(
    int     requiredMajorVersion, 
    int     requiredMinorVersion, 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
    String  feature) 
        throws StandardException;

    /**
     * The ScanManager.close() method has been called on "scan".
     * <p>
     * Take whatever cleanup action is appropriate to a closed scan.  It is
     * likely this routine will remove references to the scan object that it
     * was maintaining for cleanup purposes.
     *
     **/
    public void closeMe(ScanManager scan);

    /**
     * The ConglomerateController.close() method has been called on 
     * "conglom_control".
     * <p>
     * Take whatever cleanup action is appropriate to a closed 
     * conglomerateController.  It is likely this routine will remove
     * references to the ConglomerateController object that it was maintaining
     * for cleanup purposes.
     **/
    public void closeMe(ConglomerateController conglom_control);

    /**
     * The SortController.close() method has been called on "sort_control".
     * <p>
     * Take whatever cleanup action is appropriate to a closed 
     * sortController.  It is likely this routine will remove
     * references to the SortController object that it was maintaining
     * for cleanup purposes.
     **/
    public void closeMe(SortController sort_control);

    /**
     * Get an Internal transaction.
     * <p>
     * Start an internal transaction.  An internal transaction is a completely
     * separate transaction from the current user transaction.  All work done
     * in the internal transaction must be physical (ie. it can be undone 
     * physically by the rawstore at the page level, rather than logically 
     * undone like btree insert/delete operations).  The rawstore guarantee's
     * that in the case of a system failure all open Internal transactions are
     * first undone in reverse order, and then other transactions are undone
     * in reverse order.
     * <p>
     * Internal transactions are meant to implement operations which, if 
     * interupted before completion will cause logical operations like tree
     * searches to fail.  This special undo order insures that the state of
     * the tree is restored to a consistent state before any logical undo 
     * operation which may need to search the tree is performed.
     * <p>
     *
	 * @return The new internal transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public TransactionManager getInternalTransaction()
        throws StandardException;

    /**
     * Get the Transaction from the Transaction manager.
     * <p>
     * Access methods often need direct access to the "Transaction" - ie. the
     * raw store transaction, so give access to it.
     *
	 * @return The raw store transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Transaction getRawStoreXact()
        throws StandardException;

    /**
     * Return existing Conglomerate after doing lookup by ContainerKey
     * <p>
     * Throws exception if it can't find a matching conglomerate for the 
     * ContainerKey.
     * 
     * @return If successful returns 
     *
     * @param container_key  container key of target conglomerate.
     *
     * @exception  StandardException  Standard exception policy.
     **/
	public Conglomerate findExistingConglomerateFromKey(
    ContainerKey container_key)
		throws StandardException;
}
