/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IRowLocking2

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

package org.apache.derby.impl.store.access.btree.index;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.BTreeRowPosition;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.WaitError;

/**

**/

class B2IRowLocking2 extends B2IRowLockingRR implements BTreeLockingPolicy
{

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    B2IRowLocking2(
    Transaction             rawtran,
    int                     lock_level,
    LockingPolicy           locking_policy,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
    {
        super(rawtran, lock_level, locking_policy, base_cc, open_btree);
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */


    /**
     * Release read lock on a row.
     *
     * @param forUpdate         Is the scan for update or for read only.
     *
     **/
    public void unlockScanRecordAfterRead(
    BTreeRowPosition        pos,
    boolean                 forUpdate)
		throws StandardException
    {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(open_btree != null, "open_btree is null");

			SanityManager.ASSERT(pos.current_leaf != null , "leaf is null");

			SanityManager.ASSERT(
                pos.current_lock_row_loc != null , 
                "pos.current_lock_row_loc is null");

			SanityManager.ASSERT(
                !pos.current_lock_row_loc.isNull(), 
                "pos.current_lock_row_loc isNull()");
		}

        // always unlock in read committed, so pass false for qualified arg.
        base_cc.unlockRowAfterRead(pos.current_lock_row_loc, forUpdate, false);
    }
}
