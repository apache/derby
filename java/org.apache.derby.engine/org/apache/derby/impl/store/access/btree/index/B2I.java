/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2I

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

package org.apache.derby.impl.store.access.btree.index;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Properties;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;


import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.LeafControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;

import org.apache.derby.iapi.services.cache.ClassSize;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;


// For JavaDoc references (i.e. @see)
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;


/*
 * @derby.formatId ACCESS_B2I_V3_ID
 *
 * @derby.purpose   The tag that describes the on disk representation of the B2I
 *            conglomerate object.  Access contains no "directory" of 
 *            conglomerate information.  In order to bootstrap opening a file
 *            it encodes the factory that can open the conglomerate in the 
 *            conglomerate id itself.  There exists a single B2IFactory which
 *            must be able to read all btree format id's.  
 *
 *            This format was used for all Derby database B2I's in version
 *            10.2 and previous versions.
 *
 * @derby.upgrade   The format id of this object is currently always read from disk
 *            as the first field of the conglomerate itself.  A bootstrap
 *            problem exists as we don't know the format id of the B2I 
 *            until we are in the "middle" of reading the B2I.  Thus the
 *            base B2I implementation must be able to read and write 
 *            all formats based on the reading the 
 *            "format_of_this_conglomerate". 
 *
 *            soft upgrade to ACCESS_B2I_V4_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default collation
 *                    id needed by new format.  No code other than
 *                    readExternal and writeExternal need know about old format.
 *                write:
 *                    will never write out new format id in soft upgrade mode.
 *                    Code in readExternal and writeExternal handles writing
 *                    correct version.  Code in the factory handles making
 *                    sure new conglomerates use the B2I_v10_2 class to 
 *                    that will write out old format info.
 *
 *            hard upgrade to ACCESS_B2I_V4_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default collation
 *                    id needed by new format.
 *                write:
 *                    Only "lazy" upgrade will happen.  New format will only
 *                    get written for new conglomerate created after the 
 *                    upgrade.  Old conglomerates continue to be handled the
 *                    same as soft upgrade.
 *
 * @derby.diskLayout 
 *     format_of_this_conlgomerate(byte[])
 *     containerid(long)
 *     segmentid(int)
 *     number_of_key_fields(int)
 *     number_of_unique_columns(int)
 *     allow_duplicates(boolean)
 *     maintain_parent_links(boolean)
 *     array_of_format_ids(byte[][])
 *     baseConglomerateId(long)
 *     rowLocationColumn(int)
 *     ascend_column_info(FormatableBitSet)
 *
 */

/*
 * @derby.formatId ACCESS_B2I_V4_ID
 *
 * @derby.purpose   The tag that describes the on disk representation of the B2I
 *            conglomerate object.  Access contains no "directory" of 
 *            conglomerate information.  In order to bootstrap opening a file
 *            it encodes the factory that can open the conglomerate in the 
 *            conglomerate id itself.  There exists a single B2IFactory which
 *            must be able to read all btree format id's.  
 *
 *            This format was used for all Derby database B2I's in version 10.3.
 *
 * @derby.upgrade   The format id of this object is currently always read from disk
 *            as the first field of the conglomerate itself.  A bootstrap
 *            problem exists as we don't know the format id of the B2I 
 *            until we are in the "middle" of reading the B2I.  Thus the
 *            base B2I implementation must be able to read and write 
 *            all formats based on the reading the 
 *            "format_of_this_conglomerate". 
 *
 *            soft upgrade to ACCESS_B2I_V5_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default
 *                    isUniqueWithDuplicateNulls value of false.
 *                    No code other than readExternal and writeExternal need 
 *                    know about old format.
 *                write:
 *                    will never write out new format id in soft upgrade mode.
 *                    Code in readExternal and writeExternal handles writing
 *                    correct version.  Code in the factory handles making
 *                    sure new conglomerates use the B2I_v10_3 class
 *                    that will write out old format info.
 *
 *            hard upgrade to ACCESS_B2I_V5_ID:
 *                read:
 *                    old format is readable by current B2I implementation,
 *                    with automatic in memory creation of default
 *                    isUniqueWithDuplicateNulls value of false.
 *
 *                write:
 *                    Only "lazy" upgrade will happen.  New format will only
 *                    get written for new conglomerate created after the 
 *                    upgrade.  Old conglomerates continue to be handled the
 *                    same as soft upgrade.
 *
 * @derby.diskLayout 
 *     format_of_this_conlgomerate(byte[])
 *     containerid(long)
 *     segmentid(int)
 *     number_of_key_fields(int)
 *     number_of_unique_columns(int)
 *     allow_duplicates(boolean)
 *     maintain_parent_links(boolean)
 *     array_of_format_ids(byte[][])
 *     baseConglomerateId(long)
 *     rowLocationColumn(int)
 *     ascend_column_info(FormatableBitSet)
 *     collation_ids(compressed array of ints)
 *
 */

/*
 * @derby.formatId ACCESS_B2I_V5_ID
 *
 * @derby.purpose   The tag that describes the on disk representation of the B2I
 *            conglomerate object.  Access contains no "directory" of 
 *            conglomerate information.  In order to bootstrap opening a file
 *            it encodes the factory that can open the conglomerate in the 
 *            conglomerate id itself.  There exists a single B2IFactory which
 *            must be able to read all btree format id's.  
 *
 *            This format is the current version id of B2I and has been used 
 *            in versions of Derby after the 10.3 release.
 *
 * @derby.upgrade   This is the current version, no upgrade necessary.
 *
 * @derby.diskLayout 
 *     format_of_this_conlgomerate(byte[])
 *     containerid(long)
 *     segmentid(int)
 *     number_of_key_fields(int)
 *     number_of_unique_columns(int)
 *     allow_duplicates(boolean)
 *     maintain_parent_links(boolean)
 *     array_of_format_ids(byte[][])
 *     baseConglomerateId(long)
 *     rowLocationColumn(int)
 *     ascend_column_info(FormatableBitSet)
 *     collation_ids(compressed array of ints)
 *     isUniqueWithDuplicateNulls(boolean)
 */

/**
 * Implements an instance of a B-Tree secondary index conglomerate.
 * A B2I object has two roles.
 * <ol>
 * <li>
 * The B2I object is stored on disk, and holds the store specific
 * information needed to access/describe the conglomerate. This
 * includes information such as the format ids of the columns, 
 * the conglomerate id of the base table, the location of 
 * row location column.
 * </li>
 * <li>
 * Access to all the interfaces start by making a call off the
 * Conglomerate interface. So for instance to get a scan on the
 * conglomerate method {@link #openScan openScan} should be called.
 * </li>
 * </ol>
 */
public class B2I extends BTree
{
    private static final String PROPERTY_BASECONGLOMID = "baseConglomerateId";
    private static final String PROPERTY_ROWLOCCOLUMN  = "rowLocationColumn";

    static final int FORMAT_NUMBER = StoredFormatIds.ACCESS_B2I_V5_ID;

	/*
	** Fields of B2I.
	*/

	/**
	The id of the conglomerate which contains the base table.
	Row locations inserted into this secondary index are assumed
	to refer to that conglomerate.  Used to obtain table/row locks on the
    base table rows which the index rows point at.
	**/
	long baseConglomerateId;
//IC see: https://issues.apache.org/jira/browse/DERBY-2359

	/**
	The column id (zero-based integer index) of the column which holds the row 
    location to the base conglomerate.
	The default value of RowLocationColumn is the last key column.
    Used to obtain table/row locks on the base table rows with the index rows
    point at.
	Currently, RowLocationColumn must be the last key column.
	**/
	int rowLocationColumn;

    private static final int BASE_MEMORY_USAGE = 
        ClassSize.estimateBaseFromCatalog( B2I.class);
//IC see: https://issues.apache.org/jira/browse/DERBY-2537

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    }

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Protected locking implmentations of abtract BTree routines:
     *     getBtreeLockingPolicy
     *     lockTable
     **************************************************************************
     */

    /**
     * Create a new btree locking policy from scratch.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    protected BTreeLockingPolicy getBtreeLockingPolicy(
    Transaction             rawtran,
    int                     lock_level,
    int                     mode,
    int                     isolation_level,
    ConglomerateController  base_cc,
    OpenBTree               open_btree)
		throws StandardException
    {
        BTreeLockingPolicy ret_locking_policy = null;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                (isolation_level == 
                     TransactionController.ISOLATION_SERIALIZABLE)      ||
                (isolation_level == 
                     TransactionController.ISOLATION_REPEATABLE_READ)   ||
                (isolation_level == 
                     TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK) ||
                (isolation_level == 
                     TransactionController.ISOLATION_READ_COMMITTED)    ||
                (isolation_level == 
                     TransactionController.ISOLATION_READ_UNCOMMITTED),
                "bad isolation_level = " + isolation_level);
        }

        if (lock_level == TransactionController.MODE_TABLE)
        {
            ret_locking_policy = 
                new B2ITableLocking3(
                    rawtran,
                    lock_level,
                    rawtran.newLockingPolicy(
                        LockingPolicy.MODE_CONTAINER, 
                        isolation_level,
                        true), 
                    base_cc,
                    open_btree);
        }
        else if (lock_level == TransactionController.MODE_RECORD)
        {
            if (isolation_level == TransactionController.ISOLATION_SERIALIZABLE)
            {
                ret_locking_policy = 
                    new B2IRowLocking3(
                        rawtran,
                        lock_level,
                        rawtran.newLockingPolicy(
                            LockingPolicy.MODE_RECORD, 
                            isolation_level,
                            true), 
                        base_cc,
                        open_btree);
            }
            else if ((isolation_level == 
                        TransactionController.ISOLATION_REPEATABLE_READ))
            {
                ret_locking_policy = 
                    new B2IRowLockingRR(
                        rawtran,
                        lock_level,
                        rawtran.newLockingPolicy(
                            LockingPolicy.MODE_RECORD, 
                            isolation_level,
                            true), 
                        base_cc,
                        open_btree);
            }
            else if ((isolation_level == 
                        TransactionController.ISOLATION_READ_COMMITTED) ||
                     (isolation_level == 
                        TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK))
            {
                ret_locking_policy = 
                    new B2IRowLocking2(
                        rawtran,
                        lock_level,
                        rawtran.newLockingPolicy(
                            LockingPolicy.MODE_RECORD, 
                            isolation_level,
                            true), 
                        base_cc,
                        open_btree);
            }
            else if (isolation_level == 
                        TransactionController.ISOLATION_READ_UNCOMMITTED)
            {
                ret_locking_policy = 
                    new B2IRowLocking1(
                        rawtran,
                        lock_level,
                        rawtran.newLockingPolicy(
                            LockingPolicy.MODE_RECORD, 
                            isolation_level,
                            true), 
                        base_cc,
                        open_btree);
            }
        }


        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                ret_locking_policy != null, "ret_locking_policy == null");
        }

        return(ret_locking_policy);
    }

    /**
     * Lock the base table.
     * <p>
     * Assumes that segment of the base container is the same as the segment
     * of the btree segment.
     * <p>
     * RESOLVE - we really want to get the lock without opening the container.
     * raw store will be providing this.
     *
     * @param xact_manager Transaction to associate the lock with.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public final ConglomerateController lockTable(
    TransactionManager  xact_manager,
    int                 open_mode,
    int                 lock_level,
    int                 isolation_level)
		throws StandardException
    {
        open_mode |= TransactionController.OPENMODE_FOR_LOCK_ONLY;

        // open the base conglomerate - just to get the table lock.
        ConglomerateController cc = 
            xact_manager.openConglomerate(
                this.baseConglomerateId, false, open_mode, lock_level, 
                isolation_level);

        return(cc);
    }
    
    /**************************************************************************
	 *  Private methods of B2I, arranged alphabetically.
     **************************************************************************
     */


    private void traverseRight()
    {
        // RESOLVE - Do I have to do this???????????????

		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("not implemented.");
    }


	/*
	** Methods of B2I.
	*/

	/**
	Create an empty secondary index b-tree, using the generic b-tree to do the
    generic part of the creation process.

    This routine opens the newly created container, adds a single page, and
    makes this page the root by inserting a LeafControlRow onto this page
    at slot 0 and marking in that control row that the page is a root page.

    The following properties are specific to the b-tree secondary index:
    <UL>
    <LI> "baseConglomerateId" (integer).  The conglomerate id of the base
    conglomerate is never actually accessed by the b-tree secondary
    index implementation, it only serves as a namespace for row locks.
    This property is required.
    <LI> "rowLocationColumn" (integer).  The zero-based index into the row which
    the b-tree secondary index will assume holds a @see RowLocation of
    the base row in the base conglomerate.  This value will be used
    for acquiring locks.  In this implementation RowLocationColumn must be 
    the last key column.
    This property is required.
    </UL>

    A secondary index i (a, b) on table t (a, b, c) would have rows
    which looked like (a, b, row_location).  baseConglomerateId is set to the
    conglomerate id of t.  rowLocationColumns is set to 2.  allowsDuplicates
    would be set to false, @see BTree#create.  To create a unique
    secondary index set uniquenessColumns to 2, this means that the btree
    code will compare the key values but not the row id when determing
    uniqueness.  To create a nonunique secondary index set uniquenessColumns
    to 3, this would mean that the uniqueness test would include the row
    location and since all row locations will be unique  all rows inserted 
    into the index will be differentiated (at least) by row location.  

	@see BTree#create

	@exception StandardException Standard exception policy.
	**/
	public void create(
    TransactionManager      xact_manager,
    int                     segmentId, 
    long                    input_conglomid, 
    DataValueDescriptor[]	template, 
	ColumnOrdering[]	    columnOrder,
    int[]                   collationIds,
    Properties              properties,
	int                     temporaryFlag)
		throws StandardException
	{
        String      property_value = null;
        Transaction rawtran        = xact_manager.getRawStoreXact();

        if (properties == null)
        {
            throw(StandardException.newException(
                    SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_BASECONGLOMID));
        }

        // Get baseConglomerateId //
        property_value = properties.getProperty(PROPERTY_BASECONGLOMID);
        if (property_value == null)
        {
            throw(StandardException.newException(
                    SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_BASECONGLOMID));
        }

        if (SanityManager.DEBUG)
        {
			if (property_value == null)
            	SanityManager.THROWASSERT(
                	PROPERTY_BASECONGLOMID +
					"property not passed to B2I.create()");
        }

        baseConglomerateId = Long.parseLong(property_value);

        // Get rowLocationColumn //
        property_value = properties.getProperty(PROPERTY_ROWLOCCOLUMN);

        if (SanityManager.DEBUG)
        {
			if (property_value == null)
            	SanityManager.THROWASSERT(
                	PROPERTY_ROWLOCCOLUMN +
					"property not passed to B2I.create()");
        }

        if (property_value == null)
        {
            throw(StandardException.newException(
                    SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_BASECONGLOMID));
        }

        rowLocationColumn = Integer.parseInt(property_value);

        // Currently the row location column must be the last column (makes)
        // comparing the columns in the index easier.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(rowLocationColumn == template.length - 1, 
                "rowLocationColumn is not the last column in the index");
            SanityManager.ASSERT(
                template[rowLocationColumn] instanceof 
                    RowLocation);

            // There must be at least one key column
			if (rowLocationColumn < 1)
            	SanityManager.THROWASSERT(
					"rowLocationColumn (" + rowLocationColumn +
					") expected to be >= 1");
        }


		/* convert the sorting order information into a boolean array map.
		 * If the sorting order for the columns is not provided, we
		 * assign the default as Ascending Order.
		 * array length is equal to template length, because column order
		 * length changes whether it is unique or is non unique. store assumes
		 * template length arrays. So, we make template length array and make
		 * the last column as ascending instead of having lot of execeptions 
         * code.
		 */
		
		ascDescInfo = new boolean[template.length];
		for (int i=0 ; i < ascDescInfo.length; i++)
		{
			if (columnOrder != null && i < columnOrder.length)
				ascDescInfo[i] = columnOrder[i].getIsAscending();
			else
				ascDescInfo[i] = true;  // default values - ascending order
		}

        // get collation ids from input collation ids, store it in the 
        // conglom state.
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
        collation_ids = 
            ConglomerateUtil.createCollationIds(template.length, collationIds);
        hasCollatedTypes = hasCollatedColumns(collation_ids);
//IC see: https://issues.apache.org/jira/browse/DERBY-5367

		// Do the generic part of creating the b-tree.
		super.create(
            rawtran, segmentId, input_conglomid, template, 
            properties, getTypeFormatId(), temporaryFlag);

        // open the base conglomerate - to get the lock
        ConglomerateController base_cc = 
            xact_manager.openConglomerate(
                baseConglomerateId,
                false,
                TransactionController.OPENMODE_FOR_LOCK_ONLY, 
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);
        
        OpenBTree open_btree = new OpenBTree();

        BTreeLockingPolicy b2i_locking_policy = 
            new B2ITableLocking3(
                rawtran,
                TransactionController.MODE_TABLE,
                rawtran.newLockingPolicy(
                    LockingPolicy.MODE_CONTAINER,
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
                    TransactionController.ISOLATION_SERIALIZABLE, true), 
                base_cc, open_btree);


        // The following call will "open" the new btree.  Create is
        // an interesting case.  What we really want is read only table lock
        // on the base conglomerate and update locks on the index.  For now
        // just get the update lock on the base table, this is done by the
        // lockTable() call made by base class.

        open_btree.init(
            (TransactionManager) xact_manager,  // current user xact
            (TransactionManager) xact_manager,  // current xact
            (ContainerHandle) null,     // have init open the container.
            rawtran, 
            false,
            (ContainerHandle.MODE_FORUPDATE),
            TransactionController.MODE_TABLE,
            b2i_locking_policy,         // get table level lock.
            this,                       
            (LogicalUndo) null,         // no logical undo necessary, as 
                                        // initEmptyBtree()
                                        // work will be done single user and
                                        // rows will not move.
            (DynamicCompiledOpenConglomInfo) null);
                                        
		// Open the newly created container, and insert the first control row.
        LeafControlRow.initEmptyBtree(open_btree);

        open_btree.close();

        base_cc.close();
	}



	/*
	** Methods of Conglomerate
	*/

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
	 * @param xact_manager    The TransactionController under which this 
     *                        operation takes place.
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
	public boolean fetchMaxOnBTree(
    TransactionManager      xact_manager,
    Transaction             rawtran,
    long                    conglomId,
    int                     open_mode,
    int                     lock_level,
    LockingPolicy           locking_policy,
    int                     isolation_level,
    FormatableBitSet                 scanColumnList,
    DataValueDescriptor[]   fetchRow)
        throws StandardException
    {
        boolean row_exists;

        // row level locking implementation.

        // RESOLVE (revisit implementation after all the Xena rowlocking
        // changes have been made).  Can probably come up with single
        // path implementation.
        
        // Create a new b-tree secondary index scan.
        B2IMaxScan b2is = new B2IMaxScan();

        // Initialize it.
        b2is.init(
            xact_manager, 
            rawtran, 
            open_mode,
            lock_level,
            locking_policy,
            isolation_level,
            true /* get locks on base table as part of open */,
            scanColumnList,
            this, 
            new B2IUndo());

        row_exists = b2is.fetchMax(fetchRow);

        b2is.close();

        return(row_exists);
    }


	/**
	Bulk Load a B-tree secondary index.

	@see Conglomerate#load
	@exception StandardException Standard Derby Error policy.
	raise SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION if a duplicate 
    key is detected in the load.
	**/

	public long load(
	TransactionManager      xact_manager,
	boolean                 createConglom,
	RowLocationRetRowSource rowSource)
		 throws StandardException
	{
        long num_rows_loaded = 0;
		B2IController b2ic = new B2IController();

		try
		{
            int open_mode = TransactionController.OPENMODE_FORUPDATE;

            if (createConglom)
            {
                open_mode |=
                    (ContainerHandle.MODE_UNLOGGED |
                     ContainerHandle.MODE_CREATE_UNLOGGED);
            }

            // Do the actual open of the container in the super class.
            b2ic.init(
                xact_manager,                    // current transaction   
                xact_manager.getRawStoreXact(),  // current raw store xact
                false,                           // Not holdable
                open_mode,
                TransactionController.MODE_TABLE,
                xact_manager.getRawStoreXact().newLockingPolicy(
                    LockingPolicy.MODE_CONTAINER,
                    TransactionController.ISOLATION_SERIALIZABLE, true),
                true,
                this, 
                new B2IUndo(),
                (B2IStaticCompiledInfo) null,
                (DynamicCompiledOpenConglomInfo) null);

            num_rows_loaded = b2ic.load(xact_manager, createConglom, rowSource);

		}
		finally
		{
			b2ic.close();
		}

        return(num_rows_loaded);
	}

	/**
	Open a b-tree controller.
	@see Conglomerate#open

	@exception StandardException Standard exception policy.
	**/
	public ConglomerateController open(
    TransactionManager              xact_manager,
    Transaction                     rawtran, 
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
		// Create a new b-tree secondary index controller.
		B2IController b2ic = new B2IController();

		// Do the actual open of the container in the super class.
		b2ic.init(
            xact_manager,               // current transaction   
            rawtran,                    // current raw store transaction
//IC see: https://issues.apache.org/jira/browse/DERBY-1058
            hold,                       // holdability
            open_mode,
            lock_level,
            locking_policy,
            true,
            this, 
            new B2IUndo(),
            (B2IStaticCompiledInfo) static_info,
            dynamic_info);

		// Return it to the caller.
		return b2ic;
	}

	/**
	Open a b-tree secondary index scan controller.
	@see Conglomerate#openScan
	@see BTree#openScan

	@exception StandardException Standard exception policy.
	**/
	public ScanManager openScan(
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    int                             isolation_level,
	FormatableBitSet                         scanColumnList,
    DataValueDescriptor[]			startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]			stopKeyValue,
    int                             stopSearchOperator,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
			throws StandardException
	{
		// Create a new b-tree secondary index scan.
		B2IForwardScan b2is = new B2IForwardScan();

		// Initialize it.
		b2is.init(xact_manager, rawtran, 
                  hold,
                  open_mode,
                  lock_level,
                  locking_policy,
                  isolation_level,
                  true /* get locks on base table as part of open */,
                  scanColumnList,
                  startKeyValue, startSearchOperator,
                  qualifier,
                  stopKeyValue, stopSearchOperator, this, new B2IUndo(),
                  (B2IStaticCompiledInfo) static_info,
                  dynamic_info);

		// Return it to the caller.
		return b2is;
	}

    /**
     * Open a b-tree compress scan.
     * <p>
     * B2I does not support a compress scan.
     * <p>
	 * @see Conglomerate#defragmentConglomerate
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public ScanManager defragmentConglomerate(
//IC see: https://issues.apache.org/jira/browse/DERBY-132
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    int                             isolation_level)
			throws StandardException
	{
        throw StandardException.newException(
            SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}

	public void purgeConglomerate(
    TransactionManager              xact_manager,
    Transaction                     rawtran)
        throws StandardException
    {
        // currently on work to do in btree's for purge rows, purging
        // happens best when split is about to happen.
        return;
    }

	public void compressConglomerate(
    TransactionManager              xact_manager,
    Transaction                     rawtran)
        throws StandardException
    {
		B2IController b2ic = new B2IController();
//IC see: https://issues.apache.org/jira/browse/DERBY-132

		try
		{
            int open_mode = TransactionController.OPENMODE_FORUPDATE;

            // Do the actual open of the container in the super class.
            b2ic.init(
                xact_manager,                    // current transaction   
                xact_manager.getRawStoreXact(),  // current raw store xact
                false,                           // Not holdable
                open_mode,
                TransactionController.MODE_TABLE,
                xact_manager.getRawStoreXact().newLockingPolicy(
                    LockingPolicy.MODE_CONTAINER,
                    TransactionController.ISOLATION_SERIALIZABLE, true),
                true,
                this, 
                new B2IUndo(),
                (B2IStaticCompiledInfo) null,
                (DynamicCompiledOpenConglomInfo) null);

            b2ic.getContainer().compressContainer();

		}
		finally
		{
			b2ic.close();
		}

        return;
    }

    /**
     * Return an open StoreCostController for the conglomerate.
     * <p>
     * Return an open StoreCostController which can be used to ask about 
     * the estimated row counts and costs of ScanController and 
     * ConglomerateController operations, on the given conglomerate.
     * <p>
	 * @param xact_manager The TransactionController under which this 
     *                     operation takes place.
	 * @param rawtran  raw transaction context in which scan is managed.
     *
	 * @return The open StoreCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see StoreCostController
     **/
    public StoreCostController openStoreCost(
    TransactionManager  xact_manager,
    Transaction         rawtran)
		throws StandardException
    {
        B2ICostController b2icost = new B2ICostController();

        b2icost.init(xact_manager, this, rawtran);

        return(b2icost);
    }

	/**
	Drop this b-tree secondary index.
	@see Conglomerate#drop
	@see BTree#drop

	@exception StandardException Standard exception policy.
	**/
	public void drop(TransactionManager xact_manager)
		throws StandardException
	{
        // HACK to get around problem where index is dropped after the base
        // table.
        ConglomerateController base_cc = null;


        /* Get X table lock to make sure no thread is accessing index */
        base_cc = 
            lockTable(
                xact_manager, 
                TransactionController.OPENMODE_FORUPDATE, 
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ);

        xact_manager.getRawStoreXact().dropContainer(id);

        if (base_cc != null)
            base_cc.close();
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
    TransactionController   xact_manager,
    long                    conglomId)
		throws StandardException
    {
        return(new B2IStaticCompiledInfo(xact_manager, this));
    }

	/*
	** Methods of Storable (via Conglomerate via BTree).
	** This class is responsible for re/storing its
	** own state and calling its superclass to store its'.
	*/


	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */


	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
		return StoredFormatIds.ACCESS_B2I_V5_ID;
	}


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID format see 
     * documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
	public void writeExternal_v10_2(ObjectOutput out) 
//IC see: https://issues.apache.org/jira/browse/DERBY-2537
        throws IOException 
    {
		super.writeExternal(out);
		out.writeLong(baseConglomerateId);
		out.writeInt(rowLocationColumn);

        //write the columns ascend/descend information as bits
        FormatableBitSet ascDescBits = 
            new FormatableBitSet(ascDescInfo.length);

        for (int i = 0; i < ascDescInfo.length; i++)
        {	
            if (ascDescInfo[i])
                ascDescBits.set(i);
        }
        ascDescBits.writeExternal(out);
	}

    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID and 
     * ACCESS_B2I_V4_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
	public void writeExternal_v10_3(ObjectOutput out) 
        throws IOException 
    {
        // First part of ACCESS_B2I_V4_ID format is the ACCESS_B2I_V3_ID format.
        writeExternal_v10_2(out);
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
		if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V4_ID
                || conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID)
        {
            // Now append sparse array of collation ids
            ConglomerateUtil.writeCollationIdArray(collation_ids, out);
        }
    }


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID and 
     * ACCESS_B2I_V5_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal_v10_3 (out);
        if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID)
            out.writeBoolean (isUniqueWithDuplicateNulls());
    }

    /**
     * Restore the in-memory representation from the stream.
     * <p>
     *
     * @exception ClassNotFoundException Thrown if the stored representation 
     *                                   is serialized and a class named in 
     *                                   the stream could not be found.
     *
     * @see java.io.Externalizable#readExternal
     **/
    public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		baseConglomerateId = in.readLong();
		rowLocationColumn  = in.readInt();

        // read the column sort order info
        FormatableBitSet ascDescBits = new FormatableBitSet();
        ascDescBits.readExternal(in);
        ascDescInfo = new boolean[ascDescBits.getLength()];
        for(int i =0 ; i < ascDescBits.getLength(); i++)
            ascDescInfo[i] = ascDescBits.isSet(i);
        
        // In memory maintain a collation id per column in the template.
        collation_ids = new int[format_ids.length];
//IC see: https://issues.apache.org/jira/browse/DERBY-5367
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!hasCollatedTypes);
        }

        // initialize all the entries to COLLATION_TYPE_UCS_BASIC, 
        // and then reset as necessary.  For version ACCESS_B2I_V3_ID,
        // this is the default and no resetting is necessary.
        for (int i = 0; i < format_ids.length; i++)
            collation_ids[i] = StringDataValue.COLLATION_TYPE_UCS_BASIC;

        // initialize the unique with null setting to false, to be reset
        // below when read from disk.  For version ACCESS_B2I_V3_ID and
        // ACCESS_B2I_V4_ID, this is the default and no resetting is necessary.
        setUniqueWithDuplicateNulls(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-3330

		if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V4_ID
                || conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID)
        {
            // current format id, read collation info from disk
            if (SanityManager.DEBUG)
            {
                // length must include row location column and at least
                // one other field.
                SanityManager.ASSERT(
                    collation_ids.length >= 2, 
                    "length = " + collation_ids.length);
            }

//IC see: https://issues.apache.org/jira/browse/DERBY-5367
            hasCollatedTypes =
                    ConglomerateUtil.readCollationIdArray(collation_ids, in);
        }
        else if (conglom_format_id != StoredFormatIds.ACCESS_B2I_V3_ID)
        {
            // Currently only V3, V4 and V5 should be possible in a Derby DB.
            // Actual work for V3 is handled by default code above, so no
            // special work is necessary.

            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "Unexpected format id: " + conglom_format_id);
            }
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
        if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID) {
            setUniqueWithDuplicateNulls(in.readBoolean());
        }
	}
}
