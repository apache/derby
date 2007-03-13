/*

   Derby - Class org.apache.derby.impl.services.locks.SinglePool

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

/**
	An implementation of LockFactory that uses a single pool
	for the locks, i.e. all lock requests go through a single
	point of synchronisation.

    <BR>
	MT - Mutable - Container Object : Thread Aware
*/

public final class SinglePool extends AbstractPool
{
    /**
     * Create the <code>LockSet</code> object that keeps the locks.
     *
     * @return a <code>LockSet</code>
     */
    protected LockTable createLockTable() {
        return new LockSet(this);
    }
}
