/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.locks
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import java.util.List;

public interface Control {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	public Lockable getLockable();

	public LockControl getLockControl();

	public Lock getLock(Object compatabilitySpace, Object qualifier);

//EXCLUDE-START-lockdiag- 
	/**
		Clone this lock for the lock table information.
		Objects cloned will not be altered.
	*/
	public Control shallowClone();
//EXCLUDE-END-lockdiag- 

	public ActiveLock firstWaiter();

	public boolean isEmpty();

	public boolean unlock(Latch lockInGroup, int unlockCount);

	public void addWaiters(java.util.Dictionary waiters);

	public Lock getFirstGrant();

	public List getGranted();

	public List getWaiting();

	public boolean isGrantable(boolean otherWaiters, Object  compatabilitySpace, Object  qualifier);


}
