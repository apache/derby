/*

   Derby - Class org.apache.derby.impl.store.access.RAMTransaction

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

package org.apache.derby.impl.store.access;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.access.conglomerate.ScanControllerRowSource;
import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.apache.derby.iapi.store.access.conglomerate.SortFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.access.XATransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;

import org.apache.derby.iapi.store.access.DatabaseInstant;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.Serializable;

// debugging

public class RAMTransaction 
    implements XATransactionController, TransactionManager
{

	/**
	The corresponding raw store transaction.
	**/
	protected Transaction rawtran;

	/**
	The access manager this transaction is under.
	**/
	protected RAMAccessManager accessmanager;

	/**
	The context this transaction is being managed by.
	**/
	protected RAMTransactionContext context;

	/**
	The parent transaction if this is a nested user transaction.
	**/
	protected RAMTransaction        parent_tran;

	// XXX (nat) management of the controllers is still embryonic.
	// XXX (nat) would be nice if sort controllers were like conglom controllers
	private ArrayList<ScanManager> scanControllers;
	private ArrayList<ConglomerateController> conglomerateControllers;
	private ArrayList<Sort> sorts;
	private ArrayList<SortController> sortControllers;

    /** List of sort identifiers (represented as <code>Integer</code> objects)
     * which can be reused. Since sort identifiers are used as array indexes,
     * we need to reuse them to avoid leaking memory (DERBY-912). */
    private ArrayList<Integer> freeSortIds;

	/**
	Where to look for temporary conglomerates.
	**/
	protected HashMap<Long,Conglomerate> tempCongloms;

	/**
	Next id to use for a temporary conglomerate.
	**/
	private long nextTempConglomId = -1;

    /**
     * Set by alter table to indicate that the conglomerate cache needs to
     * be invalidated if a transaction aborting error is encountered, cleared
     * after cleanup.
     */
    private boolean alterTableCallMade = false;

    /**
     * The lock level of the transaction.
     * <p>
     * Cannot lock a level lower than the getSystemLockLevel().  So if 
     * getSystemLockLevel() is table level locking, setting the transaction
     * locking level to record has no effect.
     **/
    private int transaction_lock_level;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    private final void init(
    RAMAccessManager myaccessmanager, 
    Transaction      theRawTran,
    RAMTransaction   parent_tran)
	{
		this.rawtran            = theRawTran;
        this.parent_tran        = parent_tran;
		accessmanager           = myaccessmanager;
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		scanControllers         = new ArrayList<ScanManager>();
		conglomerateControllers = new ArrayList<ConglomerateController>();

		sorts                   = null; // allocated on demand.
		freeSortIds             = null; // allocated on demand.
		sortControllers         = null; // allocated on demand

        if (parent_tran != null)
        {
            // allow nested transactions to see temporary conglomerates which
            // were created in the parent transaction.  This is necessary for
            // language which compiling plans in nested transactions against 
            // user temporaries created in parent transactions.

            tempCongloms        = parent_tran.tempCongloms;
        }
        else
        {
            tempCongloms        = null; // allocated on demand
        }
	}

	protected RAMTransaction(
    RAMAccessManager myaccessmanager, 
    Transaction      theRawTran,
    RAMTransaction   parent_transaction)
		throws StandardException
	{
        init(myaccessmanager, theRawTran, parent_transaction);
	}

	RAMTransaction(
    RAMAccessManager myaccessmanager, 
    RAMTransaction   tc,
    int              format_id,
    byte[]           global_id,
    byte[]           branch_id)
		throws StandardException
	{
        init(myaccessmanager, tc.getRawStoreXact(), null);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(tc.getRawStoreXact().isIdle());

        this.context = tc.context;

        // switch the transaction pointer in the context to point to this xact
        this.context.setTransaction(this);

        this.rawtran.createXATransactionFromLocalTransaction(
            format_id, global_id, branch_id);

        // invalidate old tc, so caller does not use it.  Can't just call
        // destroy as that screws up the contexts which we want to just leave
        // alone.
        tc.rawtran = null;
	}


	RAMTransaction()
	{
	}


    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */


	// XXX (nat) currently closes all controllers.
	protected void closeControllers(boolean closeHeldControllers)
        throws StandardException
	{

        if (!scanControllers.isEmpty())
        {
            // loop from end to beginning, removing scans which are not held.
            for (int i = scanControllers.size() - 1; i >= 0; i--)
            {
                ScanManager sc = scanControllers.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

                if (sc.closeForEndTransaction(closeHeldControllers))
                {
                    // TODO - now counting on scan's removing themselves by 
                    // calling the closeMe() method.
                    /* scanControllers.removeElementAt(i); */
                }
            }

            if (closeHeldControllers)
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(scanControllers.isEmpty());
                }
                // just to make sure everything has been closed and removed.
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
                scanControllers.clear();
            }
        }

        if (!conglomerateControllers.isEmpty())
        {
            // loop from end to beginning, removing scans which are not held.
            for (int i = conglomerateControllers.size() - 1; i >= 0; i--)
            {
                ConglomerateController cc = conglomerateControllers.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

                if (cc.closeForEndTransaction(closeHeldControllers))
                {
                    // TODO - now counting on cc's removing themselves by 
                    // calling the closeMe() method.
                    /* conglomerateControllers.removeElementAt(i); */
                }
            }

            if (closeHeldControllers)
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(scanControllers.isEmpty());
                }
                // just to make sure everything has been closed and removed.
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
                conglomerateControllers.clear();
            }
        }

        if ((sortControllers != null) && !sortControllers.isEmpty())
        {
            if (closeHeldControllers)
            {
                // Loop from the end since the call to close() will remove the
                // element from the list.
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
                for (int i = sortControllers.size() - 1; i >= 0; i--)
                {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                    SortController sc = sortControllers.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-2486
                    sc.completedInserts();
                }
                sortControllers.clear();
            }
        }

		if ((sorts != null) && (!sorts.isEmpty()))
		{
            if (closeHeldControllers)
            {
                // Loop from the end since the call to drop() will remove the
                // element from the list.
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
                for (int i = sorts.size() - 1; i >= 0; i--)
                {
                    Sort sort = sorts.get(i);
                    if (sort != null)
                        sort.drop(this);
                }
                sorts.clear();
                freeSortIds.clear();
            }
		}
	}


    /**
     * Determine correct locking policy for a conglomerate open.
     * <p>
     * Determine from the following table whether to table or record lock
     * the conglomerate we are opening.
     * <p>
     *
     *
     *                                     System level override
     *                                     -------------------------------
     * user requests                       table locking    record locking
     * -------------                       -------------    --------------
     * TransactionController.MODE_TABLE     TABLE             TABLE
     * TransactionController.MODE_RECORD    TABLE             RECORD
     **/
    private LockingPolicy determine_locking_policy(
    int requested_lock_level,
    int isolation_level)
    {
        LockingPolicy ret_locking_policy;

        if ((accessmanager.getSystemLockLevel() == 
                TransactionController.MODE_TABLE) ||
            (requested_lock_level == TransactionController.MODE_TABLE))
        {
            ret_locking_policy = 
                accessmanager.table_level_policy[isolation_level];
        }
        else 
        {
            ret_locking_policy = 
                accessmanager.record_level_policy[isolation_level];
            
        }
        return(ret_locking_policy);
    }

    private int determine_lock_level(
    int requested_lock_level)
    {
        int ret_lock_level;

        if ((accessmanager.getSystemLockLevel() == 
                TransactionController.MODE_TABLE) ||
            (requested_lock_level == TransactionController.MODE_TABLE))
        {
            ret_lock_level = TransactionController.MODE_TABLE;
        }
        else 
        {
            ret_lock_level = TransactionController.MODE_RECORD;
            
        }
        return(ret_lock_level);
    }

	private Conglomerate findExistingConglomerate(long conglomId)
		throws StandardException
	{
		Conglomerate conglom = findConglomerate(conglomId);

		if (conglom == null)
        {
			throw StandardException.newException(
                SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                conglomId);
        }
        else
        {
            return(conglom);
        }
	}

	private Conglomerate findConglomerate(long conglomId)
		throws StandardException
	{
		Conglomerate conglom = null;

		if (conglomId >= 0)
        {
            conglom = accessmanager.conglomCacheFind(conglomId);
        }
        else
		{
			if (tempCongloms != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
				conglom = (Conglomerate) tempCongloms.get(conglomId);
		}

        return(conglom);
	}

	void setContext(RAMTransactionContext rtc)
	{
		context = rtc;
	}

    private ConglomerateController openConglomerate(
    Conglomerate                    conglom,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            if ((open_mode & 
                    ~(ContainerHandle.MODE_UNLOGGED                 |
                      ContainerHandle.MODE_CREATE_UNLOGGED          |
                      ContainerHandle.MODE_FORUPDATE                |
                      ContainerHandle.MODE_READONLY	                |
                      ContainerHandle.MODE_TRUNCATE_ON_COMMIT       |
                      ContainerHandle.MODE_DROP_ON_COMMIT           |
                      ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY       |
                      ContainerHandle.MODE_LOCK_NOWAIT              |
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
                      ContainerHandle.MODE_LOCK_ROW_NOWAIT          |
                      ContainerHandle.MODE_TRUNCATE_ON_ROLLBACK     |
                      ContainerHandle.MODE_FLUSH_ON_COMMIT          |
                      ContainerHandle.MODE_NO_ACTIONS_ON_COMMIT     |
                      ContainerHandle.MODE_TEMP_IS_KEPT		        |
                      ContainerHandle.MODE_USE_UPDATE_LOCKS	        |
                      ContainerHandle.MODE_SECONDARY_LOCKED         |
                      ContainerHandle.MODE_BASEROW_INSERT_LOCKED)) != 0)   
            {
                SanityManager.THROWASSERT(
                    "Bad open mode to openConglomerate:" + 
                        Integer.toHexString(open_mode));
            }

            SanityManager.ASSERT(conglom != null);
            
            if (lock_level != MODE_RECORD && lock_level != MODE_TABLE)
            {
                SanityManager.THROWASSERT(
                    "Bad lock level to openConglomerate:" + lock_level);
            }
        }

		// Get a conglomerate controller.
		ConglomerateController cc = 
            conglom.open(
                this, rawtran, hold, open_mode, 
                determine_lock_level(lock_level), 
                determine_locking_policy(lock_level, isolation_level),
                static_info,
                dynamic_info);

		// Keep track of it so we can release on close.
		conglomerateControllers.add(cc);
//IC see: https://issues.apache.org/jira/browse/DERBY-2149

		return cc;
    }

	private ScanController openScan(
    Conglomerate                    conglom,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]           startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]           stopKeyValue,
    int                             stopSearchOperator,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            if ((open_mode & 
                 ~(TransactionController.OPENMODE_FORUPDATE |
                   TransactionController.OPENMODE_USE_UPDATE_LOCKS |
                   TransactionController.OPENMODE_FOR_LOCK_ONLY |
                   TransactionController.OPENMODE_LOCK_NOWAIT |
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
                   TransactionController.OPENMODE_LOCK_ROW_NOWAIT |
                   TransactionController.OPENMODE_SECONDARY_LOCKED)) != 0)
            {
                SanityManager.THROWASSERT(
                    "Bad open mode to openScan:" +
                    Integer.toHexString(open_mode));
            }

            if (!((lock_level == MODE_RECORD | lock_level == MODE_TABLE)))
            {
                SanityManager.THROWASSERT(
                    "Bad lock level to openScan:" + lock_level);
            }
        }

		// Get a scan controller.
		ScanManager sm =
            conglom.openScan(
                this, rawtran, hold, open_mode,
                determine_lock_level(lock_level),
                determine_locking_policy(lock_level, isolation_level),
                isolation_level,
                scanColumnList,
                startKeyValue, startSearchOperator,
                qualifier,
                stopKeyValue, stopSearchOperator,
                static_info,
                dynamic_info);

		// Keep track of it so we can release on close.
		scanControllers.add(sm);

		return(sm);
	}

    /**
     * Invalidate the conglomerate cache, if necessary.  If an alter table
     * call has been made then invalidate the cache.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected void invalidateConglomerateCache()
        throws StandardException
    {
        if (alterTableCallMade)
        {
            accessmanager.conglomCacheInvalidate();
            alterTableCallMade = false;
        }
    }


    /**************************************************************************
     * Public Methods of TransactionController interface:
     **************************************************************************
     */

    /**
    Add a column to a conglomerate.  The conglomerate must not be open in
	the current transaction.  This also means that there must not be any
    active scans on it.

    The column can only be added at the spot just after the current set of
    columns.

    The template_column must be nullable.

    After this call has been made, all fetches of this column from rows that
    existed in the table prior to this call will return "null".

	@param conglomId        The identifier of the conglomerate to drop.
	@param column_id        The column number to add this column at.
	@param template_column  An instance of the column to be added to table.
	@param collation_id     collation id of the added column.

	@exception StandardException Only some types of conglomerates can support
        adding a column, for instance "heap" conglomerates support adding a
        column while "btree" conglomerates do not.  If the column can not be
        added an exception will be thrown.
    **/
    public void addColumnToConglomerate(
    long        conglomId,
    int         column_id,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
    Storable    template_column,
    int         collation_id)
		throws StandardException
    {
        boolean is_temporary = (conglomId < 0);

//IC see: https://issues.apache.org/jira/browse/DERBY-5632
		Conglomerate conglom = findConglomerate(conglomId);
		if (conglom == null)
        {
			throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                SQLState.AM_NO_SUCH_CONGLOMERATE_DROP, conglomId);
        }

        // Get exclusive lock on the table being altered.
		ConglomerateController cc =
            conglom.open(
                this, rawtran, false, OPENMODE_FORUPDATE,
                MODE_TABLE,
                accessmanager.table_level_policy[
                    TransactionController.ISOLATION_SERIALIZABLE],
                (StaticCompiledOpenConglomInfo) null,
                (DynamicCompiledOpenConglomInfo) null);

		conglom.addColumn(this, column_id, template_column, collation_id);
//IC see: https://issues.apache.org/jira/browse/DERBY-2537

        // Set an indication that ALTER TABLE has been called so that the
        // conglomerate will be invalidated if an error happens. Only needed
        // for non-temporary conglomerates, since they are the only ones that
        // live in the conglomerate cache.
//IC see: https://issues.apache.org/jira/browse/DERBY-5632
        if (!is_temporary)
		{
            alterTableCallMade = true;
        }

        cc.close();

        return;
    }

    /**
     * Return static information about the conglomerate to be included in a
     * a compiled plan.
     * <p>
     * The static info would be valid until any ddl was executed on the
     * conglomid, and would be up to the caller to throw away when that
     * happened.  This ties in with what language already does for other
     * invalidation of static info.  The type of info in this would be
     * containerid and array of format id's from which templates can be created.
     * The info in this object is read only and can be shared among as many
     * threads as necessary.
     * <p>
     *
	 * @return The static compiled information.
     *
     * @param conglomId The identifier of the conglomerate to open.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
    long        conglomId)
		throws StandardException
    {
        return(
            findExistingConglomerate(
                conglomId).getStaticCompiledConglomInfo(this, conglomId));
    }

    /**
     * Return dynamic information about the conglomerate to be dynamically
     * reused in repeated execution of a statement.
     * <p>
     * The dynamic info is a set of variables to be used in a given
     * ScanController or ConglomerateController.  It can only be used in one
     * controller at a time.  It is up to the caller to insure the correct
     * thread access to this info.  The type of info in this is a scratch
     * template for btree traversal, other scratch variables for qualifier
     * evaluation, ...
     * <p>
     *
	 * @return The dynamic information.
     *
     * @param conglomId The identifier of the conglomerate to open.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo(
    long        conglomId)
		throws StandardException
    {
        return(
            findExistingConglomerate(
//IC see: https://issues.apache.org/jira/browse/DERBY-2359
                conglomId).getDynamicCompiledConglomInfo());
    }


    private final int countCreatedSorts()
    {
        int ret_val = 0;
        if (sorts != null)
        {
            for (int i = 0; i < sorts.size(); i++)
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
                if (sorts.get(i) != null)
                    ret_val++;
            }
        }

        return(ret_val);
    }

    /**
     * Report on the number of open conglomerates in the transaction.
     * <p>
     * There are 4 types of open "conglomerates" that can be tracked, those
     * opened by each of the following: openConglomerate(), openScan(), 
     * openSort(), and openSortScan().  This routine can be used to either
     * report on the number of all opens, or may be used to track one 
     * particular type of open.
     *
     * This routine is expected to be used for debugging only.  An 
     * implementation may only track this info under SanityManager.DEBUG mode.
     * If the implementation does not track the info it will return -1 (so
     * code using this call to verify that no congloms are open should check
     * for return &lt;= 0 rather than == 0).
     *
     * The return value depends on the "which_to_count" parameter as follows:
     * OPEN_CONGLOMERATE  - return # of openConglomerate() calls not close()'d.
     * OPEN_SCAN          - return # of openScan() calls not close()'d.
     * OPEN_CREATED_SORTS - return # of sorts created (createSort()) in 
     *                      current xact.  There is currently no way to get
     *                      rid of these sorts before end of transaction.
     * OPEN_SORT          - return # of openSort() calls not close()'d.
     * OPEN_TOTAL         - return total # of all above calls not close()'d.
     *     - note an implementation may return -1 if it does not track the
     *       above information.
     *
	 * @return The nunber of open's of a type indicated by "which_to_count"
     *         parameter.
     *
     * @param which_to_count Which kind of open to report on.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int countOpens(int which_to_count)
		throws StandardException
	{
        int         ret_val = -1;

        switch (which_to_count)
        {
            case OPEN_CONGLOMERATE:
                ret_val = conglomerateControllers.size();
                break;
            case OPEN_SCAN:
                ret_val = scanControllers.size();
                break;
            case OPEN_CREATED_SORTS:
                ret_val = countCreatedSorts();
                break;
            case OPEN_SORT:
                ret_val = 
                    ((sortControllers != null) ? sortControllers.size() : 0);
                break;
            case OPEN_TOTAL:
                ret_val = 
                    conglomerateControllers.size() + scanControllers.size() +
                    ((sortControllers != null) ? sortControllers.size() : 0) +
                    countCreatedSorts();
                break;
        }

        return(ret_val);
	}

    /**
     * Create a new conglomerate.
     * <p>
     * @see TransactionController#createConglomerate
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public long createConglomerate(
    String                  implementation,
    DataValueDescriptor[]   template,
    ColumnOrdering[]        columnOrder,
    int[]                   collationIds,
    Properties              properties,
    int                     temporaryFlag)
		throws StandardException
	{
		// Find the appropriate factory for the desired implementation.
		MethodFactory mfactory;
		mfactory = accessmanager.findMethodFactoryByImpl(implementation);
		if (mfactory == null || !(mfactory instanceof ConglomerateFactory))
        {
			throw StandardException.newException(
                    SQLState.AM_NO_SUCH_CONGLOMERATE_TYPE, implementation);
        }
		ConglomerateFactory cfactory = (ConglomerateFactory) mfactory;

		// Create the conglomerate
        // RESOLVE (mikem) - eventually segmentid's will be passed into here
        // in the properties.  For now just use 0.]
		int     segment;
        long    conglomid;
		if ((temporaryFlag & TransactionController.IS_TEMPORARY)
				== TransactionController.IS_TEMPORARY)
        {
			segment = ContainerHandle.TEMPORARY_SEGMENT;
            conglomid = ContainerHandle.DEFAULT_ASSIGN_ID;
        }
		else
        {
			segment = 0; // RESOLVE - only using segment 0
            conglomid = 
                accessmanager.getNextConglomId(
                    cfactory.getConglomerateFactoryId());
        }

        // call the factory to actually create the conglomerate.
        Conglomerate conglom =
            cfactory.createConglomerate(
                this, segment, conglomid, template, 
                columnOrder, collationIds, properties, temporaryFlag);
//IC see: https://issues.apache.org/jira/browse/DERBY-2537

		long conglomId;
		if ((temporaryFlag & TransactionController.IS_TEMPORARY)
				== TransactionController.IS_TEMPORARY)
		{
			conglomId = nextTempConglomId--;
			if (tempCongloms == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				tempCongloms = new HashMap<Long,Conglomerate>();
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
			tempCongloms.put(conglomId, conglom);
		}
		else
		{
			conglomId = conglom.getContainerid();

            accessmanager.conglomCacheAddEntry(conglomId, conglom);
		}

		return conglomId;
	}

	/**
		Create a conglomerate and populate it with rows from rowSource.

		@see TransactionController#createAndLoadConglomerate
		@exception StandardException Standard Derby Error Policy
	*/
    public long createAndLoadConglomerate(
    String                  implementation,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    int[]                   collationIds,
    Properties              properties,
    int                     temporaryFlag,
    RowLocationRetRowSource rowSource,
	long[] rowCount)
		throws StandardException
	{
        return(
            recreateAndLoadConglomerate(
                implementation,
                true,
                template,
				columnOrder,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                collationIds,
                properties,
                temporaryFlag,
                0 /* unused if recreate_ifempty is true */,
                rowSource,
				rowCount));
	}

	/**
		recreate a conglomerate and populate it with rows from rowSource.

		@see TransactionController#createAndLoadConglomerate
		@exception StandardException Standard Derby Error Policy
	*/
    public long recreateAndLoadConglomerate(
    String                  implementation,
    boolean                 recreate_ifempty,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    int[]                   collationIds,
    Properties              properties,
    int			            temporaryFlag,
    long                    orig_conglomId,
    RowLocationRetRowSource rowSource,
	long[] rowCount)
        throws StandardException

	{
		// RESOLVE: this create the conglom LOGGED, this is slower than
		// necessary although still correct.
		long conglomId = 
			createConglomerate(
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                implementation, template, columnOrder, collationIds, 
                properties, temporaryFlag);

        long rows_loaded = 
            loadConglomerate(
                conglomId, 
                true, // conglom is being created
                rowSource);

		if (rowCount != null)
			rowCount[0] = rows_loaded;

        if (!recreate_ifempty && (rows_loaded == 0))
        {
            dropConglomerate(conglomId);

            conglomId = orig_conglomId;
        }

		return conglomId;
	}

    /**
     * Return a string with debug information about opened congloms/scans/sorts.
     * <p>
     * Return a string with debugging information about current opened
     * congloms/scans/sorts which have not been close()'d.
     * Calls to this routine are only valid under code which is conditional
     * on SanityManager.DEBUG.
     * <p>
     *
	 * @return String with debugging information.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String debugOpened() throws StandardException
    {
        String str = null;

        if (SanityManager.DEBUG)
        {

            str = "";
//IC see: https://issues.apache.org/jira/browse/DERBY-5491

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            for (Iterator<ScanManager> it = scanControllers.iterator(); it.hasNext(); )
            {
                ScanController sc = it.next();
                str += "open scan controller: " + sc + "\n";
            }

            for (Iterator<ConglomerateController> it = conglomerateControllers.iterator();
                 it.hasNext(); )
            {
                ConglomerateController cc = (ConglomerateController) it.next();
                str += "open conglomerate controller: " + cc + "\n";
            }

            if (sortControllers != null)
            {
                for (Iterator<SortController> it = sortControllers.iterator(); it.hasNext(); )
                {
                    SortController sc = it.next();
                    str += "open sort controller: " + sc + "\n";
                }
            }

            if (sorts != null)
            {
                for (int i = 0; i < sorts.size(); i++)
                {
                    Sort sort = sorts.get(i);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

                    if (sort != null)
                    {
                        str += 
                            "sorts created by createSort() in current xact:" + 
                            sort + "\n";
                    }
                }
            }

			if (tempCongloms != null)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                for (Iterator<Long> it = tempCongloms.keySet().iterator();
                     it.hasNext(); )
                {
					Long conglomId = it.next();
					Conglomerate c = tempCongloms.get(conglomId);
					str += "temp conglomerate id = " + conglomId + ": " + c;
				}
			}

        }

        return(str);
    }

    public boolean conglomerateExists(long conglomId)
		throws StandardException
	{
		Conglomerate conglom = findConglomerate(conglomId);
		if (conglom == null)
			return false;
		return true;
	}

    public void dropConglomerate(long conglomId)
		throws StandardException
	{
		Conglomerate conglom = findExistingConglomerate(conglomId);

		conglom.drop(this);

		if (conglomId < 0)
		{
			if (tempCongloms != null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
				tempCongloms.remove(conglomId);
		}
		else
        {
			accessmanager.conglomCacheRemoveEntry(conglomId);
        }
	}

    /**
     * Retrieve the maximum value row in an ordered conglomerate.
     * <p>
     * Returns true and fetches the rightmost row of an ordered conglomerate 
     * into "fetchRow" if there is at least one row in the conglomerate.  If
     * there are no rows in the conglomerate it returns false.
     * <p>
     * Non-ordered conglomerates will not implement this interface, calls
     * will generate a StandardException.
     * <p>
     * RESOLVE - this interface is temporary, long term equivalent (and more) 
     * functionality will be provided by the openBackwardScan() interface.  
     *
	 * @param conglomId       The identifier of the conglomerate
	 *                        to open the scan for.
     *
	 * @param open_mode       Specifiy flags to control opening of table.  
     *                        OPENMODE_FORUPDATE - if set open the table for
     *                        update otherwise open table shared.
     * @param lock_level      One of (MODE_TABLE, MODE_RECORD, or MODE_NONE).
     *
     * @param isolation_level The isolation level to lock the conglomerate at.
     *                        One of (ISOLATION_READ_COMMITTED or 
     *                        ISOLATION_SERIALIZABLE).
     *
	 * @param scanColumnList  A description of which columns to return from 
     *                        every fetch in the scan.  template, 
     *                        and scanColumnList work together
     *                        to describe the row to be returned by the scan - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a "row".
     *
     * @param fetchRow        The row to retrieve the maximum value into.
     *
	 * @return boolean indicating if a row was found and retrieved or not.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean fetchMaxOnBtree(
    long                    conglomId,
    int                     open_mode,
    int                     lock_level,
    int                     isolation_level,
    FormatableBitSet                 scanColumnList,
    DataValueDescriptor[]   fetchRow)
        throws StandardException
    {
		// Find the conglomerate.
		Conglomerate conglom = findExistingConglomerate(conglomId);

		// Get a scan controller.
        return(
            conglom.fetchMaxOnBTree(
                this, 
                rawtran,
                conglomId,
                open_mode,
                lock_level,
                determine_locking_policy(lock_level, isolation_level),
                isolation_level,
                scanColumnList,
                fetchRow));
    }


    /**
     * A superset of properties that "users" can specify.
     * <p>
     * A superset of properties that "users" (ie. from sql) can specify.  Store
     * may implement other properties which should not be specified by users.
     * Layers above access may implement properties which are not known at
     * all to Access.
     * <p>
     * This list is a superset, as some properties may not be implemented by
     * certain types of conglomerates.  For instant an in-memory store may not
     * implement a pageSize property.  Or some conglomerates may not support
     * pre-allocation.
     * <p>
     * This interface is meant to be used by the SQL parser to do validation
     * of properties passsed to the create table statement, and also by the
     * various user interfaces which present table information back to the 
     * user.
     * <p>
     * Currently this routine returns the following list:
     *      derby.storage.initialPages
     *      derby.storage.minimumRecordSize
     *      derby.storage.pageReservedSpace
     *      derby.storage.pageSize
     *
	 * @return The superset of properties that "users" can specify.
     *
     **/
    public Properties getUserCreateConglomPropList()
    {
        Properties  ret_properties = 
            ConglomerateUtil.createUserRawStorePropertySet((Properties) null);

        return(ret_properties);
    }

    /**
     * Reveals whether the transaction has ever read or written data.
     *
	 * @return true If the transaction has never read or written data.
     *
     **/
	public boolean isIdle()
    {
		return rawtran.isIdle();
    }

    /**
     * Reveals whether the transaction is a global or local transaction.
     *
	 * @return true If the transaction was either started by 
     *         AccessFactory.startXATransaction() or was morphed to a global
     *         transaction by calling 
     *         AccessFactory.createXATransactionFromLocalTransaction().
     * 
     * @see AccessFactory#startXATransaction
     * @see TransactionController#createXATransactionFromLocalTransaction
     *
     **/
	public boolean isGlobal()
    {
        return(rawtran.getGlobalId() != null);
    }

    /**
     * Reveals whether the transaction is currently pristine.
     *
	 * @return true If the transaction is Pristine.
     *
	 * @see TransactionController#isPristine
     **/
	public boolean isPristine()
    {
		return rawtran.isPristine();
	}

	/**
     * Convert a local transaction to a global transaction.
     * <p>
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Tbis controller allows one to manipulate a 
     * global XA conforming transaction.
     * <p>
     * Must only be called a previous local transaction was created and exists
     * in the context.  Can only be called if the current transaction is in
     * the idle state.  Upon return from this call the old tc will be unusable,
     * and all references to it should be dropped (it will have been implicitly
     * destroy()'d by this call.
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.  
     *
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/
	public /* XATransactionController */ Object 
    createXATransactionFromLocalTransaction(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id)
		throws StandardException
    {

        getRawStoreXact().createXATransactionFromLocalTransaction(
            format_id, global_id, branch_id);

        return this;
    }

	/**
		Bulk load into the conglomerate.  Rows being loaded into the
		conglomerate are not logged.

		@param conglomId The conglomerate Id.

		@param createConglom If true, the conglomerate is being created in the
		same operation as the loadConglomerate.  The enables further
		optimization as recovery does not require page allocation to be
		logged. 

		@param rowSource Where the rows come from.

	    @return true The number of rows loaded.

		@exception StandardException Standard Derby Error Policy
	 */
	public long loadConglomerate(
    long                    conglomId,
    boolean                 createConglom,
    RowLocationRetRowSource rowSource)
		throws StandardException
	{
		// Find the conglomerate.
		Conglomerate conglom = findExistingConglomerate(conglomId);

		// Load up the conglomerate with rows from the rowSource.
		// Don't need to keep track of the conglomerate controller because load
		// automatically closes it when it finished.
		return(conglom.load(this, createConglom, rowSource));
	}

	/**
		Use this for incremental load in the future.  

		@param conglomId the conglomerate Id
		@param rowSource where the rows to be loaded comes from 

		@exception StandardException Standard Derby Error Policy
	 */
	public void loadConglomerate(
    long                    conglomId,
    RowLocationRetRowSource rowSource)
		throws StandardException
	{
		loadConglomerate(
				conglomId, 
				false, // conglomerate is not being created  
				rowSource);
	}	

    /**
     * Log an operation and then action it in the context of this transaction.
     * <p>
     * This simply passes the operation to the RawStore which logs and does it.
     * <p>
     *
     * @param operation the operation that is to be applied
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void logAndDo(Loggable operation)
		throws StandardException
	{
		rawtran.logAndDo(operation);
	}

    public ConglomerateController openCompiledConglomerate(
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(static_info != null);
            SanityManager.ASSERT(dynamic_info != null);
        }
        
        // in the current implementation, only Conglomerate's are passed around
        // as StaticCompiledOpenConglomInfo.

        return(
            openConglomerate(
                (Conglomerate) static_info.getConglom(),
                hold, open_mode, lock_level, isolation_level, 
                static_info, dynamic_info));
    }

    public ConglomerateController openConglomerate(
    long                            conglomId, 
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level)
		throws StandardException
	{
        return(
            openConglomerate(
                findExistingConglomerate(conglomId),
                hold, open_mode, lock_level, isolation_level, 
                (StaticCompiledOpenConglomInfo) null,
                (DynamicCompiledOpenConglomInfo) null));
	}

    public long findConglomid(long container_id)
        throws StandardException
    {
        return(container_id);
    }

    public long findContainerid(long conglom_id)
        throws StandardException
    {
        return(conglom_id);
    }

    /**
     * Create a BackingStoreHashtable which contains all rows that qualify for
     * the described scan.
     **/
    public BackingStoreHashtable createBackingStoreHashtableFromScan(
    long                    conglomId,
    int                     open_mode,
    int                     lock_level,
    int                     isolation_level,
    FormatableBitSet                 scanColumnList,
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator,
    long                    max_rowcnt,
    int[]                   key_column_numbers,
    boolean                 remove_duplicates,
    long                    estimated_rowcnt,
    long                    max_inmemory_rowcnt,
    int                     initialCapacity,
    float                   loadFactor,
    boolean                 collect_runtimestats,
    boolean		            skipNullKeyColumns,
    boolean                 keepAfterCommit,
    boolean                 includeRowLocations )
        throws StandardException
    {
        return (
            new BackingStoreHashTableFromScan(
                this,
                conglomId,
                open_mode,
                lock_level,
                isolation_level,
                scanColumnList,
                startKeyValue,
                startSearchOperator,
                qualifier,
                stopKeyValue,
                stopSearchOperator,
                max_rowcnt,
                key_column_numbers,
                remove_duplicates,
                estimated_rowcnt,
                max_inmemory_rowcnt,
                initialCapacity,
                loadFactor,
                collect_runtimestats,
				skipNullKeyColumns,
                keepAfterCommit,
                includeRowLocations));
    }


	public GroupFetchScanController openGroupFetchScan(
    long                            conglomId,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]           startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]           stopKeyValue,
    int                             stopSearchOperator)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
			if ((open_mode & 
                ~(TransactionController.OPENMODE_FORUPDATE | 
                  TransactionController.OPENMODE_FOR_LOCK_ONLY |
                  TransactionController.OPENMODE_SECONDARY_LOCKED)) != 0)
				SanityManager.THROWASSERT(
					"Bad open mode to openScan:" + 
                    Integer.toHexString(open_mode));

			if (!(lock_level == MODE_RECORD |
                 lock_level == MODE_TABLE))
				SanityManager.THROWASSERT(
                "Bad lock level to openScan:" + lock_level);
        }

		// Find the conglomerate.
		Conglomerate conglom = findExistingConglomerate(conglomId);

		// Get a scan controller.
		ScanManager sm = 
            conglom.openScan(
                this, rawtran, hold, open_mode, 
                determine_lock_level(lock_level),
                determine_locking_policy(lock_level, isolation_level),
                isolation_level,
                scanColumnList,
                startKeyValue, startSearchOperator,
                qualifier,
                stopKeyValue, stopSearchOperator,
                (StaticCompiledOpenConglomInfo) null,
                (DynamicCompiledOpenConglomInfo) null);

		// Keep track of it so we can release on close.
		scanControllers.add(sm);

		return(sm);
	}



    /**
     * Purge all committed deleted rows from the conglomerate.
     * <p>
     * This call will purge committed deleted rows from the conglomerate,
     * that space will be available for future inserts into the conglomerate.
     * <p>
     *
     * @param conglomId Id of the conglomerate to purge.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void purgeConglomerate(
//IC see: https://issues.apache.org/jira/browse/DERBY-132
    long    conglomId)
        throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
        findExistingConglomerate(conglomId).purgeConglomerate
            ( this, rawtran );

		return;
    }

    /**
     * Return free space from the conglomerate back to the OS.
     * <p>
     * Returns free space from the conglomerate back to the OS.  Currently
     * only the sequential free pages at the "end" of the conglomerate can
     * be returned to the OS.
     * <p>
     *
     * @param conglomId Id of the conglomerate to purge.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void compressConglomerate(
    long    conglomId)
        throws StandardException
    {
        findExistingConglomerate(conglomId).compressConglomerate(
            this, 
            rawtran); 

		return;
    }

    /**
     * Compress table in place.
     * <p>
     * Returns a GroupFetchScanController which can be used to move rows
     * around in a table, creating a block of free pages at the end of the
     * table.  The process will move rows from the end of the table toward
     * the beginning.  The GroupFetchScanController will return the 
     * old row location, the new row location, and the actual data of any
     * row moved.  Note that this scan only returns moved rows, not an
     * entire set of rows, the scan is designed specifically to be
     * used by either explicit user call of the SYSCS_ONLINE_COMPRESS_TABLE()
     * procedure, or internal background calls to compress the table.
     *
     * The old and new row locations are returned so that the caller can
     * update any indexes necessary.
     *
     * This scan always returns all collumns of the row.
     * 
     * All inputs work exactly as in openScan().  The return is 
     * a GroupFetchScanController, which only allows fetches of groups
     * of rows from the conglomerate.
     * <p>
     *
	 * @return The GroupFetchScanController to be used to fetch the rows.
     *
	 * @param conglomId             see openScan()
     * @param hold                  see openScan()
     * @param open_mode             see openScan()
     * @param lock_level            see openScan()
     * @param isolation_level       see openScan()
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see ScanController
     * @see GroupFetchScanController
     **/
	public GroupFetchScanController defragmentConglomerate(
    long                            conglomId,
    boolean                         online,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level)
        throws StandardException
	{
        if (SanityManager.DEBUG)
        {
			if ((open_mode & 
                ~(TransactionController.OPENMODE_FORUPDATE | 
                  TransactionController.OPENMODE_FOR_LOCK_ONLY |
                  TransactionController.OPENMODE_SECONDARY_LOCKED)) != 0)
				SanityManager.THROWASSERT(
					"Bad open mode to openScan:" + 
                    Integer.toHexString(open_mode));

			if (!(lock_level == MODE_RECORD |
                 lock_level == MODE_TABLE))
				SanityManager.THROWASSERT(
                "Bad lock level to openScan:" + lock_level);
        }

		// Find the conglomerate.
		Conglomerate conglom = findExistingConglomerate(conglomId);

		// Get a scan controller.
		ScanManager sm = 
            conglom.defragmentConglomerate(
                this, 
                rawtran, 
                hold, 
                open_mode, 
                determine_lock_level(lock_level),
                determine_locking_policy(lock_level, isolation_level),
                isolation_level);

		// Keep track of it so we can release on close.
		scanControllers.add(sm);
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
//IC see: https://issues.apache.org/jira/browse/DERBY-2149

		return(sm);
	}


	public ScanController openScan(
    long                            conglomId,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]           startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]           stopKeyValue,
    int                             stopSearchOperator)
        throws StandardException
	{
        return(
            openScan(
                findExistingConglomerate(conglomId),
                hold,
                open_mode,
                lock_level,
                isolation_level,
                scanColumnList,
                startKeyValue,
                startSearchOperator,
                qualifier,
                stopKeyValue,
                stopSearchOperator,
                (StaticCompiledOpenConglomInfo) null,
                (DynamicCompiledOpenConglomInfo) null));
    }

	public ScanController openCompiledScan(
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]           startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]           stopKeyValue,
    int                             stopSearchOperator,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException
	{
        // in the current implementation, only Conglomerate's are passed around
        // as StaticCompiledOpenConglomInfo.

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(static_info != null);
            SanityManager.ASSERT(dynamic_info != null);
            SanityManager.ASSERT(
                static_info instanceof StaticCompiledOpenConglomInfo);
            SanityManager.ASSERT(
                dynamic_info instanceof DynamicCompiledOpenConglomInfo);
        }

        return(
            openScan(
                ((Conglomerate) static_info.getConglom()),
                hold,
                open_mode,
                lock_level,
                isolation_level,
                scanColumnList,
                startKeyValue,
                startSearchOperator,
                qualifier,
                stopKeyValue,
                stopSearchOperator,
                static_info,
                dynamic_info));
	}


    /**
     * Return an open StoreCostController for the given conglomid.
     * <p>
     * Return an open StoreCostController which can be used to ask about 
     * the estimated row counts and costs of ScanController and 
     * ConglomerateController operations, on the given conglomerate.
     * <p>
     *
	 * @return The open StoreCostController.
     *
     * @param conglomId The identifier of the conglomerate to open.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see StoreCostController
     **/
    public StoreCostController openStoreCost(
    long        conglomId)
		throws StandardException
    {
		// Find the conglomerate.
		Conglomerate conglom = findExistingConglomerate(conglomId);

		// Get a scan controller.
		StoreCostController scc = conglom.openStoreCost(this, rawtran);

		return(scc);
    }

	/**
	@see TransactionController#createSort
	@exception StandardException Standard error policy.
	**/
	public long createSort(
    Properties              implParameters,
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    SortObserver	        sortObserver,
    boolean                 alreadyInOrder,
    long                    estimatedRows,
    int                     estimatedRowSize)
        throws StandardException
	{
		// Get the implementation type from the parameters.
		// XXX (nat) need to figure out how to select sort implementation.
		String implementation = null;
		if (implParameters != null)
			implementation = 
                implParameters.getProperty(AccessFactoryGlobals.IMPL_TYPE);

		if (implementation == null)
			implementation = AccessFactoryGlobals.SORT_EXTERNAL;

		// Find the appropriate factory for the desired implementation.
		MethodFactory mfactory;
		mfactory = accessmanager.findMethodFactoryByImpl(implementation);
		if (mfactory == null || !(mfactory instanceof SortFactory))
        {
			throw(
              StandardException.newException(
                  SQLState.AM_NO_FACTORY_FOR_IMPLEMENTATION, implementation));
        }
		SortFactory sfactory = (SortFactory) mfactory;

		// Decide what segment the sort should use.
		int segment = 0; // XXX (nat) sorts always in segment 0

		// Create the sort.
		Sort sort = sfactory.createSort(this, segment,
						implParameters,	template, columnOrdering,
						sortObserver, alreadyInOrder, estimatedRows, 
                        estimatedRowSize);

		// Add the sort to the sorts vector
		if (sorts == null) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			sorts = new ArrayList<Sort>();
            freeSortIds = new ArrayList<Integer>();
        }

        int sortid;
        if (freeSortIds.isEmpty()) {
            // no free identifiers, add sort at the end
            sortid = sorts.size();
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
            sorts.add(sort);
        } else {
            // reuse a sort identifier
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            sortid = (freeSortIds.remove(freeSortIds.size() - 1))
                .intValue();
            sorts.set(sortid, sort);
        }

		return sortid;
	}

	/**
	Drop a sort. 
    <p>
    Drop a sort created by a call to createSort() within the current 
    transaction (sorts are automatically "dropped" at the end of a 
    transaction.  This call should only be made after all openSortScan()'s
    and openSort()'s have been closed.
    <p>

    @param sortid The identifier of the sort to drop, as returned from 
                  createSort.
 	@exception StandardException From a lower-level exception.
	**/
    public void dropSort(long sortid) 
        throws StandardException
    {
        // should call close on the sort.
        Sort sort = sorts.get((int) sortid);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

        if (sort != null)
        {
            sort.drop(this);
            sorts.set((int) sortid, null);
//IC see: https://issues.apache.org/jira/browse/DERBY-6885
            freeSortIds.add((int) sortid);
        }
    }

    /**
	@see TransactionController#getProperty
	@exception  StandardException  Standard exception policy.
    **/
    public Serializable getProperty(
    String key) 
        throws StandardException
	{
		return 
            (accessmanager.getTransactionalProperties().getProperty(this, key));
	}

	    /**
	@see TransactionController#getPropertyDefault
	@exception  StandardException  Standard exception policy.
    **/
    public Serializable getPropertyDefault(
    String key) 
        throws StandardException
	{
		return 
            (accessmanager.getTransactionalProperties().getPropertyDefault(this, key));
	}

    /**
	@see TransactionController#setProperty
	@exception  StandardException  Standard exception policy.
    **/
    public void	setProperty(
    String       key, 
    Serializable value,
	boolean dbOnlyProperty) 
        throws StandardException
	{
		accessmanager.getTransactionalProperties().setProperty(
            this, key, value, dbOnlyProperty);
	}

    /**
	@see TransactionController#setProperty
	@exception  StandardException  Standard exception policy.
    **/
    public void	setPropertyDefault(
    String       key, 
    Serializable value) 
        throws StandardException
	{
		accessmanager.getTransactionalProperties().setPropertyDefault(
            this, key, value);
	}

    /**
	@see TransactionController#propertyDefaultIsVisible
	@exception  StandardException  Standard exception policy.
    **/
    public boolean propertyDefaultIsVisible(String key) throws StandardException
	{
		return accessmanager.getTransactionalProperties().propertyDefaultIsVisible(this,key);
	}

    /**
	@see TransactionController#getProperties
	@exception  StandardException  Standard exception policy.
    **/
    public Properties	getProperties() 
        throws StandardException
	{
		return accessmanager.getTransactionalProperties().getProperties(this);
	}

	/**
	@see TransactionController#openSort
	@exception StandardException Standard error policy.
	**/
	public SortController openSort(long id)
		throws StandardException
	{
		Sort sort;

		// Find the sort in the sorts list, throw an error
		// if it doesn't exist.
		if (sorts == null || id >= sorts.size()
			|| (sort = (sorts.get((int) id))) == null)
		{
			throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    SQLState.AM_NO_SUCH_SORT, id);
		}

		// Open it.
		SortController sc = sort.open(this);

		// Keep track of it so we can release on close.
		if (sortControllers == null)
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			sortControllers = new ArrayList<SortController>();
		sortControllers.add(sc);

		return sc;
	}

    /**
     * Return an open SortCostController.
     * <p>
     * Return an open SortCostController which can be used to ask about 
     * the estimated costs of SortController() operations.
     * <p>
     *
	 * @return The open StoreCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see StoreCostController
     **/
    public SortCostController openSortCostController()
		throws StandardException
    {
		// Get the implementation type from the parameters.
		// RESOLVE (mikem) need to figure out how to select sort implementation.
		String implementation = null;

		if (implementation == null)
			implementation = AccessFactoryGlobals.SORT_EXTERNAL;

		// Find the appropriate factory for the desired implementation.
		MethodFactory mfactory;
		mfactory = accessmanager.findMethodFactoryByImpl(implementation);
		if (mfactory == null || !(mfactory instanceof SortFactory))
        {
			throw(
              StandardException.newException(
                  SQLState.AM_NO_FACTORY_FOR_IMPLEMENTATION, implementation));
        }
		SortFactory sfactory = (SortFactory) mfactory;

		// open sort cost controller
        return(sfactory.openSortCostController());
    }

	/**
	@see TransactionController#openSortScan
	@exception StandardException Standard error policy.
	**/
	public ScanController openSortScan(
    long    id,
    boolean hold)
		throws StandardException
	{
		Sort sort;

		// Find the sort in the sorts list, throw an error
		// if it doesn't exist.
		if (sorts == null || id >= sorts.size()
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
			|| (sort = ((Sort) sorts.get((int) id))) == null)
		{
			throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    SQLState.AM_NO_SUCH_SORT, id);
		}

		// Open a scan on it.
		ScanManager sc = sort.openSortScan(this, hold);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		// Keep track of it so we can release on close.
		scanControllers.add(sc);
//IC see: https://issues.apache.org/jira/browse/DERBY-2149

		return sc;
	}

	/**
	@see TransactionController#openSortRowSource
	@exception StandardException Standard error policy.
	**/
	public RowLocationRetRowSource openSortRowSource(long id) 
		 throws StandardException
	{
		Sort sort;

		// Find the sort in the sorts list, throw an error
		// if it doesn't exist.
		if (sorts == null || id >= sorts.size()
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			|| (sort = (sorts.get((int) id))) == null)
		{
			throw StandardException.newException(
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    SQLState.AM_NO_SUCH_SORT, id);
		}

		// Open a scan row source on it.
		ScanControllerRowSource sc = sort.openSortRowSource(this);

		// Keep track of it so we can release on close.
		scanControllers.add( (ScanManager) sc );
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		return sc;
	}


	public void commit()
		throws StandardException
	{
		this.closeControllers(false /* don't close held controllers */ );

        rawtran.commit();

        alterTableCallMade = false;

        return;
	}

	public DatabaseInstant commitNoSync(int commitflag)
		throws StandardException
	{
		this.closeControllers(false /* don't close held controllers */ );
		return rawtran.commitNoSync(commitflag);
	}

	public void abort()
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-5632
        invalidateConglomerateCache();
		this.closeControllers(true /* close all controllers */ );
		rawtran.abort();

        if (parent_tran != null)
            parent_tran.abort();
	}

    /**
     * Get the context manager that the transaction was created with.
     * <p>
     *
	 * @return The context manager that the transaction was created with.
     *     **/
    public ContextManager getContextManager()
    {
        return(context.getContextManager());
    }

	public int setSavePoint(String name, Object kindOfSavepoint)
		throws StandardException
	{
		return rawtran.setSavePoint(name, kindOfSavepoint);
	}

	public int releaseSavePoint(String name, Object kindOfSavepoint)
		throws StandardException
	{
		return rawtran.releaseSavePoint(name, kindOfSavepoint);
	}

	public int rollbackToSavePoint(String name, boolean close_controllers, Object kindOfSavepoint)
		throws StandardException
	{
        if (close_controllers)
            this.closeControllers(true /* close all controllers */ );
		return rawtran.rollbackToSavePoint(name, kindOfSavepoint);
	}

	public void destroy()
	{
		try
		{
			this.closeControllers(true /* close all controllers */);
			
			// If there's a transaction, abort it.
			if (rawtran != null) {
				rawtran.destroy();
				rawtran = null;
			}
			

			// If there's a context, pop it.
			if (context != null)
				context.popMe();
			context = null;

			accessmanager = null;
			tempCongloms = null;
		}
		catch (StandardException e)
		{
			// XXX (nat) really need to figure out what to do
			// if there's an exception while aborting.
			rawtran = null;
			context = null;
			accessmanager = null;
			tempCongloms = null;
		}
	}

	public boolean anyoneBlocked()
	{
		return rawtran.anyoneBlocked();
	}

    /**************************************************************************
     * Public Methods implementing the XATransactionController interface.
     **************************************************************************
     */

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
     *
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 current xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_commit(
    boolean onePhase)
		throws StandardException
    {
        rawtran.xa_commit(onePhase);
    }

    /**
     * This method is called to ask the resource manager to prepare for
     * a transaction commit of the transaction specified in xid.
     * <p>
     *
     * @return         A value indicating the resource manager's vote on the
     *                 the outcome of the transaction.  The possible values
     *                 are:  XA_RDONLY or XA_OK.  If the resource manager wants
     *                 to roll back the transaction, it should do so by 
     *                 throwing an appropriate XAException in the prepare
     *                 method.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int xa_prepare()
		throws StandardException
    {
        return(rawtran.xa_prepare());
    }

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_rollback()
        throws StandardException
    {
        rawtran.xa_rollback();
    }

    /**************************************************************************
     * Public Methods of TransactionManager interface:
     **************************************************************************
     */

    /**
     * Return existing Conglomerate after doing lookup by ContainerKey
     * <p>
     * Throws exception if it can't find a matching conglomerate for the 
     * ContainerKey.
     * 
     * @return If successful returns 
     *
     * @param container_key  container key of target conglomerate.
     *
     * @exception  StandardException  Standard exception policy.
     **/
	public Conglomerate findExistingConglomerateFromKey(
    ContainerKey container_key)
		throws StandardException
    {
        // in this implementation of the store conglomerate id's and 
        // container id's are the same.
        return(findExistingConglomerate(container_key.getContainerId()));
	}

    /**
     * Add to the list of post commit work.
     * <p>
     * Add to the list of post commit work that may be processed after this
     * transaction commits.  If this transaction aborts, then the post commit
     * work list will be thrown away.  No post commit work will be taken out
     * on a rollback to save point.
     * <p>
     * This routine simply delegates the work to the Rawstore transaction.
     *
     * @param work  The post commit work to do.
     *
     **/
	public void addPostCommitWork(Serviceable work)
    {
        rawtran.addPostCommitWork(work);

        return;
    }

    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature               Non-null to throw an exception, null to 
     *                              return the state of the version match.
     *
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     *
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
	public boolean checkVersion(
    int     requiredMajorVersion, 
    int     requiredMinorVersion, 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
    String  feature) 
        throws StandardException
    {
        return(
            accessmanager.getRawStore().checkVersion(
                requiredMajorVersion, requiredMinorVersion, feature));
    }

    /**
     * The ConglomerateController.close() method has been called on 
     * "conglom_control".
     * <p>
     * Take whatever cleanup action is appropriate to a closed 
     * conglomerateController.  It is likely this routine will remove
     * references to the ConglomerateController object that it was maintaining
     * for cleanup purposes.
     *
     **/
    public void closeMe(ConglomerateController conglom_control)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
        conglomerateControllers.remove(conglom_control);
    }

    /**
     * The SortController.close() method has been called on "sort_control".
     * <p>
     * Take whatever cleanup action is appropriate to a closed 
     * sortController.  It is likely this routine will remove
     * references to the SortController object that it was maintaining
     * for cleanup purposes.
     **/
    public void closeMe(SortController sort_control)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
        sortControllers.remove(sort_control);
    }

    /**
     * The ScanManager.close() method has been called on "scan".
     * <p>
     * Take whatever cleanup action is appropriate to a closed scan.  It is
     * likely this routine will remove references to the scan object that it
     * was maintaining for cleanup purposes.
     *
     **/
    public void closeMe(ScanManager scan)
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2149
        scanControllers.remove(scan);
    }

    /**
     * Get reference to access factory which started this transaction.
     * <p>
     *
	 * @return The AccessFactory which started this transaction.
     *
     **/
    public AccessFactory getAccessManager()
    {
        return(accessmanager);
    }

    /**
     * Get an Internal transaction.
     * <p>
     * Start an internal transaction.  An internal transaction is a completely
     * separate transaction from the current user transaction.  All work done
     * in the internal transaction must be physical (ie. it can be undone 
     * physically by the rawstore at the page level, rather than logically 
     * undone like btree insert/delete operations).  The rawstore guarantee's
     * that in the case of a system failure all open Internal transactions are
     * first undone in reverse order, and then other transactions are undone
     * in reverse order.
     * <p>
     * Internal transactions are meant to implement operations which, if 
     * interupted before completion will cause logical operations like tree
     * searches to fail.  This special undo order insures that the state of
     * the tree is restored to a consistent state before any logical undo 
     * operation which may need to search the tree is performed.
     * <p>
     *
	 * @return The new internal transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public TransactionManager getInternalTransaction()
        throws StandardException
    {
        // Get the context manager.
        ContextManager cm = getContextManager();

        // Allocate a new transaction no matter what.
        
        // Create a transaction, make a context for it, and push the context.
        // Note this puts the raw store transaction context
        // above the access context, which is required for
        // error handling assumptions to be correct.
        
        Transaction rawtran = 
            accessmanager.getRawStore().startInternalTransaction(cm);
        RAMTransaction rt   = new RAMTransaction(accessmanager, rawtran, null);
        RAMTransactionContext rtc   = 
			new RAMTransactionContext(
                cm, AccessFactoryGlobals.RAMXACT_INTERNAL_CONTEXT_ID, 
                rt, true /*abortAll */);

        rawtran.setDefaultLockingPolicy(
                accessmanager.getDefaultLockingPolicy());

        return(rt);
    }

    /**
     * Get an nested user transaction.
     * <p>
     * A nested user can be used exactly as any other TransactionController,
     * except as follows.  For this discussion let the parent transaction 
     * be the transaction used to make the getNestedUserTransaction(), and
     * let the child transaction be the transaction returned by the 
     * getNestedUserTransaction() call.
     * <p>
     * The nesting is limited to one level deep.  An exception will be thrown
     * if a subsequent getNestedUserTransaction() is called on the child
     * transaction.
     * <p>
     * The locks in the child transaction will be compatible with the locks
     * of the parent transaction.
     * <p>
     * A commit in the child transaction will release locks associated with
     * the child transaction only, work can continue in the parent transaction
     * at this point.  
     * <p>
     * Any abort of the child transaction will result in an abort of both
     * the child transaction and parent transaction.
     * <p>
     * A TransactionController.destroy() call should be made on the child
     * transaction once all child work is done, and the caller wishes to 
     * continue work in the parent transaction.
     * <p>
     * Nested internal transactions are meant to be used to implement 
     * system work necessary to commit as part of implementing a user's
     * request, but where holding the lock for the duration of the user
     * transaction is not acceptable.  2 examples of this are system catalog
     * read locks accumulated while compiling a plan, and auto-increment.
     * <p>
     *
     * @param readOnly                 Is transaction readonly?  Only 1 non-read
     *                                 only nested transaction is allowed per 
     *                                 transaction.
     *
     * @param flush_log_on_xact_end    By default should the transaction commit
     *                                 and abort be synced to the log.  Normal
     *                                 usage should pick true, unless there is
     *                                 specific performance need and usage 
     *                                 works correctly if a commit can be lost
     *                                 on system crash.
     *
	 * @return The new nested user transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public TransactionController startNestedUserTransaction(
    boolean readOnly,
    boolean flush_log_on_xact_end)
        throws StandardException
    {
        // Get the context manager.
        ContextManager cm = getContextManager();

        // Allocate a new transaction no matter what.
        
        // Create a transaction, make a context for it, and push the context.
        // Note this puts the raw store transaction context
        // above the access context, which is required for
        // error handling assumptions to be correct.
        //
        // Note that the nested transaction inherits the compatibility space
        // from "this", thus the new transaction shares the compatibility space
        // of the current transaction.
        

        Transaction child_rawtran = 
            ((readOnly) ?
                accessmanager.getRawStore().startNestedReadOnlyUserTransaction(
//IC see: https://issues.apache.org/jira/browse/DERBY-6554
                    rawtran, 
                    getLockSpace(), 
                    cm,
                    AccessFactoryGlobals.NESTED_READONLY_USER_TRANS) :
                accessmanager.getRawStore().startNestedUpdateUserTransaction(
                    rawtran, 
                    cm, 
                    AccessFactoryGlobals.NESTED_UPDATE_USER_TRANS,
                    flush_log_on_xact_end));

        RAMTransaction rt   = 
            new RAMTransaction(accessmanager, child_rawtran, this);

        RAMTransactionContext rtc   = 
			new RAMTransactionContext(
                cm, 
                AccessFactoryGlobals.RAMXACT_CHILD_CONTEXT_ID,
                rt, true /*abortAll */);

        child_rawtran.setDefaultLockingPolicy(
                accessmanager.getDefaultLockingPolicy());

        return(rt);
    }

    /**
     * Get the Transaction from the Transaction manager.
     * <p>
     * Access methods often need direct access to the "Transaction" - ie. the
     * raw store transaction, so give access to it.
     *
	 * @return The raw store transaction.
     *
     **/
    public Transaction getRawStoreXact()
	{
        return(rawtran);
    }

	public FileResource getFileHandler() {
		return rawtran.getFileHandler();
	}

    /**
     * Return an object that when used as the compatibility space,
     * <strong>and</strong> the object returned when calling
     * <code>getOwner()</code> on that object is used as group for a lock
     * request, guarantees that the lock will be removed on a commit or an
     * abort.
     */
    public CompatibilitySpace getLockSpace()
    {
		return rawtran.getCompatibilitySpace();
	}

    /**
     * {@inheritDoc}
     *
     * <p>
     *
     * For now, this only works if the transaction has its own compatibility
     * space. If it has inherited the compatibility space from its parent,
     * the request will be ignored (or cause a failure in debug builds).
     */
    public void setNoLockWait(boolean noWait) {
        rawtran.setNoLockWait(noWait);
    }

    /**
     * Get string id of the transaction.
     * <p>
     * This transaction "name" will be the same id which is returned in
     * the TransactionInfo information, used by the lock and transaction
     * vti's to identify transactions.
     * <p>
     * Although implementation specific, the transaction id is usually a number
     * which is bumped every time a commit or abort is issued.
     * <p>
     * For now return the toString() method, which does what we want.  Later
     * if that is not good enough we can add public raw tran interfaces to
     * get exactly what we want.
     *
	 * @return The a string which identifies the transaction.  
     **/
    public String getTransactionIdString()
    {
        return(rawtran.toString());
    }


    /**
     * Get string id of the transaction that would be when the Transaction
	 * is IN active state.
     **/
    public String getActiveStateTxIdString()
    {
		return(rawtran.getActiveStateTxIdString());
	}


    public String toString()
    {
        String str = null;

        if (SanityManager.DEBUG)
        {
            str = "rawtran = " + rawtran;
        }
        return(str);
    }
}
