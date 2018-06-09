/*

   Derby - Class org.apache.derby.impl.services.locks.Control

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

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.locks.Latch;
import java.util.List;
import java.util.Map;

public interface Control {

	public Lockable getLockable();

	public LockControl getLockControl();

	public Lock getLock(CompatibilitySpace compatibilitySpace,
						Object qualifier);

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

	public void addWaiters(Map<Object,Object> waiters);

	public Lock getFirstGrant();

	public List getGranted();

	public List getWaiting();

	public boolean isGrantable(boolean otherWaiters,
							   CompatibilitySpace compatibilitySpace,
							   Object qualifier);


}
