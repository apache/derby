/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;

/**
	An RePreparable operation is an operation that changed the state of the 
    RawStore in the context of a transaction and the locks for this change
    can be reclaimed during recovery, following redo.

*/

public interface RePreparable 
{
    /**
     * reclaim locks associated with the changes in this log record.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void reclaimPrepareLocks(
    Transaction     t,   
    LockingPolicy   locking_policy)
		throws StandardException;
}
