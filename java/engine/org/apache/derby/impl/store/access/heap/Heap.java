/*

   Derby - Class org.apache.derby.impl.store.access.heap.Heap

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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


package org.apache.derby.impl.store.access.heap;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Properties;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RawStoreFactory;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.cache.ClassSize;

import org.apache.derby.iapi.types.DataType;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.GenericConglomerate;
import org.apache.derby.impl.store.access.conglomerate.OpenConglomerate;
import org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * @format_id ACCESS_HEAP_V1_ID
 *
 * @purpose   The tag that describes the on disk representation of the Heap
 *            conglomerate object.  The Heap conglomerate object is stored in
 *            a field of a row in the Conglomerate directory.
 *
 * @upgrade   This format was made obsolete in the kimono release.
 *
 * @disk_layout
 *     containerid(long)
 *     segmentid(int)
 **/

/**
 * @format_id ACCESS_HEAP_V2_ID
 *
 * @purpose   The tag that describes the on disk representation of the Heap
 *            conglomerate object.  The Heap conglomerate object is stored in
 *            a field of a row in the Conglomerate directory.
 *
 * @upgrade   The format id of this object is currently always read from disk
 *            as a separate column in the conglomerate directory.  To read
 *            A conglomerate object from disk and upgrade it to the current
 *            version do the following:
 *
 *                format_id = get format id from a separate column
 *                Upgradable conglom_obj = instantiate empty obj(format_id)
 *                read in conglom_obj from disk
 *                conglom = conglom_obj.upgradeToCurrent();
 *
 * @disk_layout
 *     format_of_this_conlgomerate(byte[])
 *     containerid(long)
 *     segmentid(int)
 *     number_of_columns(int)
 *     array_of_format_ids(byte[][])
 **/

/**

  A heap object corresponds to an instance of a heap conglomerate.  It caches
  information which makes it fast to open heap controllers from it.

**/

public final class Heap 
    extends    GenericConglomerate
    implements Conglomerate, StaticCompiledOpenConglomInfo
{

	/*
	** Fields of Heap.
	*/

	protected ContainerKey id;

    /**
     * The format id's of each of the columns in the heap table.
     **/
    int[]    format_ids;

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( Heap.class);
    private static final int CONTAINER_KEY_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( ContainerKey.class);

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE;

        if( null != id)
            sz += CONTAINER_KEY_MEMORY_USAGE;
        if( null != format_ids)
            sz += format_ids.length*ClassSize.getIntSize();
        return sz;
    } // end of estimateMemoryUsage

	/*
	** Methods of Heap.
	*/

    /* Constructors for This class: */

    /**
     * Zero arg. constructor for Monitor to create empty object.
     **/
    public Heap()
    {
    }

    /* Private/Protected methods of This class: */

    /**
     * Create a heap conglomerate.
     * <p>
     * Create a heap conglomerate.  This method is called from the heap factory
     * to create a new instance of a heap.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	protected void create(
    Transaction             rawtran,
    int                     segmentId,
    long                    input_containerid,
    DataValueDescriptor[]   template,
    Properties              properties,
	int                     tmpFlag)
		throws StandardException
	{
		// Create a container for the heap table with
		// default minimumRecordSize to be at least
		// MINIMUM_RECORD_SIZE_DEFAULT (12),
		// to guarantee there is enough room for updates
		// of the row.
		// Here we only take care of the case that
		// that the properties are set with the create
		// statement.  For the case when properties are
		// not set with the create statement, it is taken
		// care of in fileContainer.java: createInfoFromProp().
		if (properties != null) 
        {
			String value = properties.getProperty(
				RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER);

			int minimumRecordSize =
				(value == null) ? 
                    RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT : 
                    Integer.parseInt(value);

			if (minimumRecordSize < RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT)
			{
				properties.put(
                    RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,
					Integer.toString(
                        RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT));
			}
		}

		// Create a container for the heap with default page size.
		long containerid = 
            rawtran.addContainer(
                segmentId, input_containerid, 
                ContainerHandle.MODE_DEFAULT, properties, tmpFlag);

		// Make sure the container was actually created.
		if (containerid < 0)
        {
            throw StandardException.newException(
                    SQLState.HEAP_CANT_CREATE_CONTAINER);
        }

		// Keep track of what segment the container's in.
		id = new ContainerKey(segmentId, containerid);

		// Heap requires a template representing every column in the table.
        if ((template == null) || (template.length == 0))
        {
            throw StandardException.newException(
                    SQLState.HEAP_COULD_NOT_CREATE_CONGLOMERATE);
        }

        // get format id's from each column in template and store it in the
        // conglomerate state.
        this.format_ids = ConglomerateUtil.createFormatIds(template);


        // need to open the container and insert the row.  Since we are
        // creating it no need to bother with locking since no one can get
        // to it until after we have created it and returned it's id.
        ContainerHandle container = null;
        Page            page      = null;

        try
        {
            container = 
                rawtran.openContainer(
                    id, (LockingPolicy) null, 
                    ContainerHandle.MODE_FORUPDATE | 
                        (isTemporary() ? ContainerHandle.MODE_TEMP_IS_KEPT : 0));

            // row in slot 0 of heap page 1 which is just a single column with
            // the heap entry.
            DataValueDescriptor[] control_row = new DataValueDescriptor[1];
            control_row[0] = this;

            page =
                container.getPage(ContainerHandle.FIRST_PAGE_NUMBER);

            page.insertAtSlot(
                Page.FIRST_SLOT_NUMBER,
                control_row,
                (FormatableBitSet) null,
                (LogicalUndo) null, 
                Page.INSERT_OVERFLOW,
                AccessFactoryGlobals.HEAP_OVERFLOW_THRESHOLD);
            page.unlatch();
            page = null;

            // Don't include the control row in the estimated row count.
            container.setEstimatedRowCount(0, /* unused flag */ 0);
        }
        finally
        {
            if (container != null)
                container.close();
            if (page !=null)
                page.unlatch();
        }
	}

    /**
     * Create a heap conglomerate during the boot process.
     * <p>
     * Manufacture a Heap Conglomerate out of "thin" air, to boot strap
     * the system.  Create an in-memory Heap Conglomerate with the input
     * parameters, The caller will use this to open the conglomerate
     * conglomerate and read the "real" values from disk.  Conglom-conglom
     * is always on segment 0.
     *
     *
     * @param containerid The container id of the conglomerate.
     * @param template    Object array describing the columns of the heap.
     **/
    public void boot_create(
    long                    containerid,
    DataValueDescriptor[]   template)
    {
		id = new ContainerKey(0, containerid);
        this.format_ids = ConglomerateUtil.createFormatIds(template);
    }

	/*
	** Methods of Conglomerate
	*/

    /**
     * Add a column to the heap conglomerate.
     * <p>
     * This routine update's the in-memory object version of the Heap
     * Conglomerate to have one more column of the type described by the
     * input template column.  
     * 
     * @param column_id        The column number to add this column at.
     * @param template_column  An instance of the column to be added to table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void addColumn(
	TransactionManager  xact_manager,
    int                 column_id,
    Storable            template_column)
        throws StandardException
    {
        // need to open the container and update the row containing the 
        // serialized format of the heap.  
        ContainerHandle container = null;
        Page            page      = null;
        Transaction     rawtran   = xact_manager.getRawStoreXact();

        try
        {
            container = 
                rawtran.openContainer(
                    id, 
                    rawtran.newLockingPolicy(
                        LockingPolicy.MODE_CONTAINER,
                        TransactionController.ISOLATION_SERIALIZABLE, true),
                    ContainerHandle.MODE_FORUPDATE | 
                        (isTemporary() ? ContainerHandle.MODE_TEMP_IS_KEPT : 0));

            if (column_id != format_ids.length)
            {
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT(
                        "column_id = " + column_id +
                        "format_ids.length = " + format_ids.length +
                        "format_ids = " + format_ids);

                throw(StandardException.newException(
                        SQLState.HEAP_TEMPLATE_MISMATCH,
                        new Long(column_id), 
                        new Long(this.format_ids.length)));
            }

            // create a new array, and copy old values to it.
            int[] old_format_ids = format_ids;
            format_ids              = new int[old_format_ids.length + 1];
            System.arraycopy(
                old_format_ids, 0, format_ids, 0, old_format_ids.length);

            // add the new column
            format_ids[old_format_ids.length] = 
                template_column.getTypeFormatId();

           
            // row in slot 0 of heap page 1 which is just a single column with
            // the heap entry.
            DataValueDescriptor[] control_row = new DataValueDescriptor[1];
            control_row[0] = this;

            page =
                container.getPage(ContainerHandle.FIRST_PAGE_NUMBER);

            page.updateAtSlot(
                Page.FIRST_SLOT_NUMBER,
                control_row,
                (FormatableBitSet) null);

            page.unlatch();
            page = null;
        }
        finally
        {
            if (container != null)
                container.close();
            if (page !=null)
                page.unlatch();
        }

        return;
    }


	/**
	Drop this heap.
	@see Conglomerate#drop

	@exception StandardException Standard exception policy.
	**/
	public void drop(TransactionManager xact_manager)
		throws StandardException
	{
        xact_manager.getRawStoreXact().dropContainer(id);
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
        // no support for max on a heap table.
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

    /**
     * Get the id of the container of the conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * container.  The ContainerKey is a combination of the container id
     * and segment id.
     *
	 * @return The ContainerKey.
     **/
    public final ContainerKey getId()
    {
        return(id);
    }


    public final long getContainerid()
    {
        return(id.getContainerId());
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
        return(new OpenConglomerateScratchSpace(format_ids));
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
    TransactionController   tc,
    long                    conglomId)
		throws StandardException
    {
        return(this);
    }


    /**
     * Is this conglomerate temporary?
     * <p>
     *
	 * @return whether conglomerate is temporary or not.
     **/
    public boolean isTemporary()
    {
        return(id.getSegmentId() == ContainerHandle.TEMPORARY_SEGMENT);
    }


    /**
     * Bulk load into the conglomerate.
     * <p>
     *
     * @see Conglomerate#load
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public long load(
	TransactionManager      xact_manager,
	boolean                 createConglom,
	RowLocationRetRowSource rowSource)
		 throws StandardException
	{
        long num_rows_loaded = 0;

		HeapController heapcontroller = new HeapController();

		try
		{
			num_rows_loaded = 
                heapcontroller.load(
                    xact_manager,
                    this, 
                    createConglom,
                    rowSource);
		}
		finally
		{
			// Done with this heap controller.
			heapcontroller.close();
		}

        return(num_rows_loaded);
	}

    /**
     * Open a heap controller.
     * <p>
     *
	 * @see Conglomerate#open
     *
	 * @exception  StandardException  Standard exception policy.
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
        OpenConglomerate open_conglom = new OpenHeap();

        if (open_conglom.init(
                (ContainerHandle) null,
                this,
                this.format_ids,
                xact_manager,
                rawtran,
                hold,
                open_mode,
                lock_level,
                locking_policy,
                dynamic_info) == null)
        {
            throw StandardException.newException(
                    SQLState.HEAP_CONTAINER_NOT_FOUND, 
                    new Long(id.getContainerId()).toString());
        }

		HeapController heapcontroller = new HeapController();

        heapcontroller.init(open_conglom);

		return(heapcontroller);
	}

    /**
     * Open a heap scan controller.
     * <p>
     *
     * @see Conglomerate#openScan
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public ScanManager openScan(
    TransactionManager              xact_manager,
    Transaction                     rawtran,
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    int                             isolation_level,
	FormatableBitSet				            scanColumnList,
    DataValueDescriptor[]	        startKeyValue,
    int                             startSearchOperator,
    Qualifier                       qualifier[][],
    DataValueDescriptor[]	        stopKeyValue,
    int                             stopSearchOperator,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
        // Heap scans do not suppport start and stop scan positions (these
        // only make sense for ordered storage structures).
		if (!RowUtil.isRowEmpty(startKeyValue, (FormatableBitSet) null)
			|| !RowUtil.isRowEmpty(stopKeyValue, (FormatableBitSet) null))
		{
            throw StandardException.newException(
                    SQLState.HEAP_UNIMPLEMENTED_FEATURE);
		}

        OpenConglomerate open_conglom = new OpenHeap();

        if (open_conglom.init(
                (ContainerHandle) null,
                this,
                this.format_ids,
                xact_manager,
                rawtran,
                hold,
                open_mode,
                lock_level,
                locking_policy,
                dynamic_info) == null)
        {
            throw StandardException.newException(
                    SQLState.HEAP_CONTAINER_NOT_FOUND, 
                    new Long(id.getContainerId()));
        }

		HeapScan heapscan = new HeapScan();

        heapscan.init(
            open_conglom,
            scanColumnList,
            startKeyValue,
            startSearchOperator,
            qualifier,
            stopKeyValue,
            stopSearchOperator);

		return(heapscan);
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
        OpenHeap open_conglom = new OpenHeap();

        if (open_conglom.init(
                (ContainerHandle) null,
                this,
                this.format_ids,
                xact_manager,
                rawtran,
                false,
                ContainerHandle.MODE_READONLY,
                TransactionController.MODE_TABLE,
                (LockingPolicy) null,
                (DynamicCompiledOpenConglomInfo) null) == null)
        {
            throw StandardException.newException(
                    SQLState.HEAP_CONTAINER_NOT_FOUND, 
                    new Long(id.getContainerId()));
        }


        HeapCostController heapcost = new HeapCostController();

        heapcost.init(open_conglom);

		return(heapcost);
    }


    /**
     * Print this heap.
     **/
    public String toString()
    {
        return (id == null) ? "null" : id.toString();
    }

    /**************************************************************************
     * Public Methods of StaticCompiledOpenConglomInfo Interface:
     **************************************************************************
     */

    /**
     * return the "Conglomerate"
     * <p>
     * For heap just return "this", which both implements Conglomerate and
     * StaticCompiledOpenConglomInfo.
     * <p>
     *
	 * @return this
     **/
    public DataValueDescriptor getConglom()
    {
        return(this);
    }


    /**************************************************************************
	 * Methods of Storable (via Conglomerate)
	 * Storable interface, implies Externalizable, TypedFormat
     **************************************************************************
     */

    /**
     * Return my format identifier.
     *
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
     **/
	public int getTypeFormatId()
    {
		return StoredFormatIds.ACCESS_HEAP_V2_ID;
	}

    /**
     * Return whether the value is null or not.
     *
	 * @see org.apache.derby.iapi.services.io.Storable#isNull
     **/
	public boolean isNull()
	{
		return id == null;
	}

    /**
     * Restore the in-memory representation to the null value.
     *
     * @see org.apache.derby.iapi.services.io.Storable#restoreToNull
     *
     **/
	public void restoreToNull()
	{
		id = null;
	}

    /**
     * Store the stored representation of the column value in the stream.
     *
     **/
	public void writeExternal(ObjectOutput out) throws IOException
    {

        // write the format id of this conglomerate
        FormatIdUtil.writeFormatIdInteger(out, this.getTypeFormatId());

		out.writeInt((int) id.getSegmentId());
        out.writeLong(id.getContainerId());

        // write number of columns in heap.
        out.writeInt(format_ids.length);

        // write out array of format id's
        ConglomerateUtil.writeFormatIdArray(format_ids, out);
	}

    /**
     * Restore the in-memory representation from the stream.
     *
     * @see java.io.Externalizable#readExternal
     **/
	public void readExternal(ObjectInput in) throws IOException 
    {
        // read the format id of this conglomerate.
        FormatIdUtil.readFormatIdInteger(in);

		int segmentid = in.readInt();
        long containerid = in.readLong();

		id = new ContainerKey(segmentid, containerid);

        // read the number of columns in the heap.
        int num_columns = in.readInt();

        // read the array of format ids.
        format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
    }

	public void readExternalFromArray(ArrayInputStream in) throws IOException 
    {
        // read the format id of this conglomerate.
        FormatIdUtil.readFormatIdInteger(in);

		int segmentid = in.readInt();
        long containerid = in.readLong();

		id = new ContainerKey(segmentid, containerid);

        // read the number of columns in the heap.
        int num_columns = in.readInt();

        // read the array of format ids.
        format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
    }

}
