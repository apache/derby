/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.btree.index
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.btree.index;

import java.io.IOException;
import java.util.Properties;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.btree.BTreeController;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

  A B2I controller object is the concrete class which corresponds to an open
  b-tree secondary index.

**/

public class B2IController extends BTreeController
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/*
	** Fields of B2IController.
	*/
    private ConglomerateController  base_cc_for_locking;

	/*
	** Methods of B2IController.
	*/

	B2IController()
	{
		// Perform the generic b-tree construction.
		super();
	}

	void init(
    TransactionManager              xact_manager,
    Transaction                     rawtran, 
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    boolean                         get_locks,
    B2I                             conglomerate,
    B2IUndo                         undo,
    B2IStaticCompiledInfo           static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
        // open and lock the base table.

        int base_open_mode = 
            open_mode | TransactionController.OPENMODE_FOR_LOCK_ONLY;

        // open the base conglomerate - just to get the lock.  Since btree
        // controllers only support update operations we just hard code 
        // the TransactionController.ISOLATION_REPEATABLE_READ, which is only
        // used for geting the IX intent lock on the table.
        if (static_info != null)
        {
            base_cc_for_locking  = 
                xact_manager.openCompiledConglomerate(
                    false,
                    base_open_mode, lock_level,
                    TransactionController.ISOLATION_REPEATABLE_READ,
                    static_info.base_table_static_info, 
                    /* TODO - maintain a dynamic info for this */
                    ((Conglomerate) static_info.getConglom()).
                        getDynamicCompiledConglomInfo(
                            conglomerate.baseConglomerateId));
        }
        else
        {
            base_cc_for_locking  = 
                xact_manager.openConglomerate(
                    conglomerate.baseConglomerateId,
                    false,
                    base_open_mode, lock_level,
                    TransactionController.ISOLATION_REPEATABLE_READ);
        }
        
        BTreeLockingPolicy b2i_locking_policy;
        if (lock_level == TransactionController.MODE_TABLE)
        {
            b2i_locking_policy = 
                new B2ITableLocking3(
                    rawtran, lock_level, locking_policy, 
                    base_cc_for_locking, this);
        }
        else if (lock_level == TransactionController.MODE_RECORD)
        {
            b2i_locking_policy = 
                new B2IRowLocking3(
                    rawtran, lock_level, locking_policy, 
                    base_cc_for_locking, this);
        }
        else
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT("Bad lock level: " + lock_level);
            }
            b2i_locking_policy = null;
        }

		// Do generic b-tree initialization.
		super.init(
            xact_manager, 
            (ContainerHandle) null, 
            rawtran, 
            open_mode,
            lock_level,
            b2i_locking_policy,
            conglomerate, 
            undo,
            static_info,
            dynamic_info);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(conglomerate != null);
	}

	/*
	** Methods of ConglomerateController.
	*/

    /**
    Close the conglomerate controller
	<p>
	Any changes to this method will probably have to be reflected in close as 
    well.
	<p>
	Currently delegates to OpenBTree.  If the btree controller ends up not 
    having any state of its own, we can remove this method (the VM will 
    dispatch to OpenBTree), gaining some small efficiency.  For now, this 
    method remains for clarity.  

	@see ConglomerateController#close
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
    Insert a row into the conglomerate.
	@see ConglomerateController#insert

    @exception StandardException Standard exception policy.
    **/
    public int insert(DataValueDescriptor[] row)
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            if (this.container != null)
            {
                SanityManager.ASSERT(this.getConglomerate() instanceof B2I);

                RowLocation rowloc = (RowLocation)
                    row[((B2I)(this.getConglomerate())).rowLocationColumn];

                SanityManager.ASSERT(
                    !rowloc.isNull(), "RowLocation value is null");
            }
        }

        return(super.insert(row));
	}
}
