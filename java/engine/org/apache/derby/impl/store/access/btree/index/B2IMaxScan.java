/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IMaxScan

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
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.GenericScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.impl.store.access.btree.BTreeController;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.BTreeMaxScan;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;

/**

  A B2I controller object is the concrete class which corresponds to an open
  b-tree secondary index.

**/

public class B2IMaxScan extends BTreeMaxScan
{

	/*
	** Fields of B2IMaxScan.
	*/
    private ConglomerateController  base_cc_for_locking;

	/*
	** Methods of B2IMaxScan.
	*/

	B2IMaxScan()
	{
		// Perform the generic b-tree scan construction.
		super();
	}

    /**
    Close the scan.
	@see GenericScanController#newRowLocationTemplate
    **/
    public void close()
        throws StandardException
	{
		super.close();

        if (base_cc_for_locking != null)
        {
            base_cc_for_locking.close();
            base_cc_for_locking = null;
        }
	}

    /**
    Close the scan, a commit or abort is about to happen.
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
        throws StandardException
	{
		boolean ret_val = super.closeForEndTransaction(closeHeldScan);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(
                ret_val, "B2IMaxScan never should be held across a commit.");

        if (base_cc_for_locking != null)
        {
            base_cc_for_locking.close();
            base_cc_for_locking = null;
        }

        return(ret_val);
	}

	/**
	Initialize the scan for use.
	<p>
	Any changes to this method may have to be reflected in close as well.
    <p>
    The btree init opens the container (super.init), and stores away the
    state of the qualifiers.  The actual searching for the first position
    is delayed until the first next() call.

	@exception  StandardException  Standard exception policy.
	**/
	public void init(
    TransactionManager  xact_manager,
    Transaction         rawtran,
    int                 open_mode,
    int                 lock_level,
    LockingPolicy       locking_policy,
    int                 isolation_level,
    boolean             open_for_locking,
    FormatableBitSet             scanColumnList,
    B2I                 conglomerate,
    B2IUndo             undo)
        throws StandardException
	{
        // open and lock the base table.

        int base_open_mode = 
            open_mode | TransactionController.OPENMODE_FOR_LOCK_ONLY;

        // open the base conglomerate - just to get lock
        base_cc_for_locking = 
            xact_manager.openConglomerate(
                conglomerate.baseConglomerateId, false, 
                base_open_mode, lock_level,
                isolation_level);
        
        BTreeLockingPolicy b2i_locking_policy = 
            conglomerate.getBtreeLockingPolicy(
                rawtran, lock_level, open_mode, isolation_level, 
                base_cc_for_locking, this);

		super.init(
            xact_manager,
            rawtran,
            false,
            open_mode,
            lock_level,
            b2i_locking_policy,
            scanColumnList,
            (DataValueDescriptor[]) null,// no start position supported
            ScanController.NA,           // no start position supported
            (Qualifier[][]) null,        // no qualifier supported
            (DataValueDescriptor[]) null,// no stop position supported
            ScanController.NA,           // no stop position supported
            conglomerate,
            undo,
            (StaticCompiledOpenConglomInfo) null,
            (DynamicCompiledOpenConglomInfo) null);
	}
}
