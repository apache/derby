/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.locks;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.VirtualLockTable;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Hashtable;

public class ShExLockable implements Lockable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	public ShExLockable()
	{		
	}

	/** @see Lockable#lockerAlwaysCompatible */
	public boolean lockerAlwaysCompatible()
	{
		return true;
	}

	/** @see Lockable#requestCompatible */
	public boolean requestCompatible(Object requestedQualifier,
											Object grantedQualifier)
	{
		if (SanityManager.DEBUG)
		{
			if (!(requestedQualifier instanceof ShExQual))
				SanityManager.THROWASSERT(
				"requestedQualifier is a " +
				requestedQualifier.getClass().getName() +
				"instead of a ShExQual.");

			if (!(grantedQualifier instanceof ShExQual))
				SanityManager.THROWASSERT(
				"grantedQualifier is a " +
				grantedQualifier.getClass().getName() +
				"instead of a ShExQual.");
		}

		ShExQual requested = (ShExQual) requestedQualifier;
		ShExQual granted = (ShExQual) grantedQualifier;

		return (requested.getLockState() == ShExQual.SHARED) &&
				(granted.getLockState() == ShExQual.SHARED);
	}

	/** @see Lockable#lockEvent */
	public void lockEvent(Latch lockInfo)
	{
		if (SanityManager.DEBUG)
		{
			if (!(lockInfo.getQualifier() instanceof ShExQual))
				SanityManager.THROWASSERT("qualifier is a " + lockInfo.getQualifier().getClass().getName() +
				"instead of a ShExQual.");
		}
	}

	/** @see Lockable#unlockEvent */
	public void unlockEvent(Latch lockInfo)
	{
		if (SanityManager.DEBUG)
		{
			if (!(lockInfo.getQualifier() instanceof ShExQual))
				SanityManager.THROWASSERT("qualifier is a " + lockInfo.getQualifier().getClass().getName() +
				"instead of a ShExQual.");
		}
	}

    /**
     * This lockable want to participate in the Virtual LockTable
     * when we want to print LATCH information.
     * Any lockable object which DOES NOT want to participate should
     * override this function.
     */
	public boolean lockAttributes(int flag, Hashtable attributes)
	{
        if((flag & VirtualLockTable.SHEXLOCK) == 0)
            return false;
        // No containerId, but need something in there so it can print
		attributes.put(VirtualLockTable.CONTAINERID, new Long(-1) ); 

		attributes.put(VirtualLockTable.LOCKNAME, this.toString() );

		attributes.put(VirtualLockTable.LOCKTYPE, "ShExLockable");

		return true;
	}

}
