/*

   Derby - Class org.apache.derby.iapi.store.access.XATransactionController

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.TransactionController;

/**

This interface allows access to commit,prepare,abort global transactions
as part of a two phase commit protocol, during runtime.  
These interfaces have been chosen to be exact implementations required to 
implement the XAResource interfaces as part of the JTA standard extension.
<P>
It is expected that the following interfaces are only used during the 
runtime portion of a 2 phase commit connection.
<P>
If a runtime exception causes a transaction abort (of a transaction that
has not been successfully prepared), then the transaction will act as if 
xa_rollback() had been called.  The transaction will be aborted and any
other call other than destroy will throw exceptions.
<P>
The XAResource interface is a Java mapping of the industry standard XA resource
manager interface.  Please refer to: X/Open CAE Specification - Distributed 
Transaction Processing: The XA Specification, X/Open Document No. XO/CAE/91/300
or ISBN 1 872630 24 3.
<P>
NOTE - all calls to this interface assume that the caller has insured that
there is no active work being done on the local instance of the transaction 
in question.  RESOLVE - not sure whether this means that the connection 
associated with the transaction must be closed, or if it just means that
synchronization has been provided to provide correct MT behavior from above.

**/

public interface XATransactionController extends TransactionController
{
    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
    public static final int XA_RDONLY = 1;
    public static final int XA_OK     = 2;

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * Once this call has been made all other calls on this controller other
     * than destroy will throw exceptions.
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
		throws StandardException;

    /**
     * This method is called to ask the resource manager to prepare for
     * a transaction commit of the transaction specified in xid.
     * <p>
     * If XA_OK is returned then any call other than xa_commit() or xa_abort()
     * will throw exceptions.  If XA_RDONLY is returned then any call other
     * than destroy() will throw exceptions.
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
		throws StandardException;

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     * Once this call has been made all other calls on this controller other
     * than destroy will throw exceptions.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_rollback()
        throws StandardException;
}
