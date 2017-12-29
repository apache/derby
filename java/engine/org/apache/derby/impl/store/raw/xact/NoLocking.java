/*

   Derby - Class org.apache.derby.impl.store.raw.xact.NoLocking

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

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.shared.common.error.StandardException;


/**
	A locking policy that implements no locking.

	@see LockingPolicy
*/
class NoLocking implements LockingPolicy {

	protected NoLocking() {
	}

	public boolean lockContainer(
    Transaction     t, 
    ContainerHandle container, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException {
		return true;
	}

	public void unlockContainer(
    Transaction     t, 
    ContainerHandle container)
    {
	}

	public boolean lockRecordForRead(
    Transaction     t, 
    ContainerHandle container,
    RecordHandle    record, 
    boolean         waitForLock,
    boolean         forUpdate)
		throws StandardException
	{
		return true;
	}

	public boolean zeroDurationLockRecordForWrite(
    Transaction     t, 
    RecordHandle    record,
    boolean         lockForPreviousKey,
    boolean         waitForLock)
		throws StandardException
	{
		return true;
	}

	public boolean lockRecordForWrite(
    Transaction     t, 
    RecordHandle    record, 
    boolean         lockForInsert,
    boolean         waitForLock)
		throws StandardException
	{
		return true;
	}

	public void unlockRecordAfterRead(
    Transaction     t, 
    ContainerHandle container,
    RecordHandle    record, 
    boolean         forUpdate,
    boolean         row_qualified)
        throws StandardException
	{
	}

	public int getMode() {
		return LockingPolicy.MODE_NONE;
	}

}
