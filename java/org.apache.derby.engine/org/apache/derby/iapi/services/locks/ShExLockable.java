/*

   Derby - Class org.apache.derby.iapi.services.locks.ShExLockable

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

package org.apache.derby.iapi.services.locks;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.VirtualLockTable;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.util.Hashtable;

public class ShExLockable implements Lockable
{

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
	public boolean lockAttributes(int flag, Hashtable<String,Object> attributes)
	{
        if((flag & VirtualLockTable.SHEXLOCK) == 0)
            return false;
        // No containerId, but need something in there so it can print
		attributes.put(VirtualLockTable.CONTAINERID, -1); 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856

		attributes.put(VirtualLockTable.LOCKNAME, this.toString() );

		attributes.put(VirtualLockTable.LOCKTYPE, "ShExLockable");

		return true;
	}

}
