/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.xact
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.locks.Latch;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerLock;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.RowLock;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException;


/**
	A locking policy that implements row level locking with repeatable read
    isolation.  Since phantom protection with previous key locking is actually 
    handled by the upper level access methods, the only difference in repeatable
    read is that read locks are of type RowLock.RS2.  This type will not 
    conflict with a previous key insert lock.

	@see org.apache.derby.iapi.store.raw.LockingPolicy
*/
public class RowLockingRR extends RowLocking3 
{
	/**
		IBM Copyright &copy notice.
	*/
 
    private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

    protected RowLockingRR(LockFactory lf)
    {
            super(lf);
    }

	protected RowLock getReadLockType() 
    {
		return(RowLock.RS2);
    }

	protected RowLock getUpdateLockType() 
    {
		return(RowLock.RU2);
    }

	protected RowLock getWriteLockType() 
    {
		return(RowLock.RX2);
    }

    /**
     * Unlock a record after it has been locked for read.
     * <p>
     * In repeatable read only unlock records which "did not qualify".  For
     * example in a query like "select * from foo where a = 1" on a table
     * with no index it is only necessary to hold locks on rows where a=1, but
     * in the process of finding those rows the system will get locks on other
     * rows to verify they are committed before applying the qualifier.  Those
     * locks can be released under repeatable read isolation.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container_handle,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
        if (!row_qualified)
        {
            Object qualifier = forUpdate ? RowLock.RU2 : RowLock.RS2;

            int count = 
                lf.unlock(t.getCompatibilitySpace(), t, record, qualifier);

            if (SanityManager.DEBUG)
            {
                // in the case of lock escalation the count could be 0.
                if (!(count == 1 || count == 0))
                {
                    SanityManager.THROWASSERT(
                    "count = " + count +
                    "record.getContainerId() = " + record.getContainerId());
                }
            }
        }
	}

}
