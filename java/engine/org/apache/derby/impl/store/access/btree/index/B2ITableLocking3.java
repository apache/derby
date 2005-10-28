/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2ITableLocking3

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.WaitError;

/**

**/

class B2ITableLocking3 extends B2INoLocking
{
    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public B2ITableLocking3(
    Transaction             rawtran,
    int                     lock_level,
    LockingPolicy           locking_policy,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
    {
    }

    private B2ITableLocking3()
    {
    }
}
