/*

   Derby - Class org.apache.derby.iapi.store.raw.RePreparable

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
