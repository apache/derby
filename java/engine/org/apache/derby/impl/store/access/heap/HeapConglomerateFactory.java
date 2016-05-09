/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapConglomerateFactory

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

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.impl.store.access.conglomerate.RowPosition;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.UUID;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;

// For JavaDoc references (i.e. @see)
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;

/**

  The heap conglomerate factory manages heap conglomerates implemented
  on the raw store.

**/

public class HeapConglomerateFactory implements ConglomerateFactory, ModuleControl, ModuleSupportable
{

	private static final String IMPLEMENTATIONID = "heap";
	private static final String FORMATUUIDSTRING = "D2976090-D9F5-11d0-B54D-00A024BF8878";
	private UUID formatUUID;

	/*
	 * Methods of MethodFactory (via ConglomerateFactory)
	 */

	/**
	Return the default properties for this kind of conglomerate.
	@see MethodFactory#defaultProperties
	**/
	public Properties defaultProperties()
	{
		// Heap has no properties.
		return new Properties();
	}

	/**
	Return whether this access method implements the implementation
	type given in the argument string.
	The heap only has one implementation type, "heap".

	@see MethodFactory#supportsImplementation
	**/
	public boolean supportsImplementation(String implementationId)
	{
		return implementationId.equals(IMPLEMENTATIONID);
	}

	/**
	Return the primary implementation type for this access method.
	The heap only has one implementation type, "heap".

	@see MethodFactory#primaryImplementationType
	**/
	public String primaryImplementationType()
	{
		return IMPLEMENTATIONID;
	}

	/**
	Return whether this access method supports the format supplied in
	the argument.
	The heap currently only supports one format, HEAPFORMAT1.

	@see MethodFactory#supportsFormat
	**/
	public boolean supportsFormat(UUID formatid)
	{
		return formatid.equals(formatUUID);
	}

	/**
	Return the primary format that this access method supports.
	The heap currently only supports one format, HEAPFORMAT1.

	@see MethodFactory#primaryFormat
	**/
	public UUID primaryFormat()
	{
		return formatUUID;
	}

	/*
	** Methods of ConglomerateFactory
	*/

    /**
     * Return the conglomerate factory id.
     * <p>
     * Return a number in the range of 0-15 which identifies this factory.
     * Code which names conglomerates depends on this range currently, but
     * could be easily changed to handle larger ranges.   One hex digit seemed
     * reasonable for the number of conglomerate types being currently 
     * considered (heap, btree, gist, gist btree, gist rtree, hash, others? ).
     * <p>
	 * @see ConglomerateFactory#getConglomerateFactoryId
     *
	 * @return an unique identifier used to the factory into the conglomid.
     *
     **/
    public int getConglomerateFactoryId()
    {
        return(ConglomerateFactory.HEAP_FACTORY_ID);
    }

	/**
	Create the conglomerate and return a conglomerate object for it.

	@exception StandardException Standard exception policy.

	@see ConglomerateFactory#createConglomerate
	**/
	public Conglomerate createConglomerate(	
    TransactionManager      xact_mgr,
    int                     segment,
    long                    input_containerid,
    DataValueDescriptor[]   template,
	ColumnOrdering[]        columnOrder,
    int[]                   collationIds,
    Properties              properties,
	int                     temporaryFlag)
		throws StandardException
	{
		Heap heap = null;


        if ((temporaryFlag & TransactionController.IS_TEMPORARY) != 0 &&
                xact_mgr.getAccessManager().isReadOnly())
        {
            // If this is a temporary conglomerate created for a read-only
            // database, we don't really care which disk format we use, since
            // it is not used for persisting data in the database. Use the
            // current format. A special case is needed because checkVersion()
            // throws an exception in read-only databases (DERBY-2354).
            heap = new Heap();
        }
        else if (xact_mgr.checkVersion(
                RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10,
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_3,
                null))
        {
            // on disk databases with version higher than 10.2 should use
            // current disk format B2I.  This includes new databases or
            // hard upgraded databases.
            heap = new Heap();
        }
        else
        {
            // Old databases that are running in new versions of the software,
            // but are running in soft upgrade mode at release level 10.2
            // and before should use the old B2I version.  This version will
            // continue to write metadata that can be read by 10.2 and previous
            // versions.
            heap = new Heap_v10_2();
        }

		heap.create(
            xact_mgr.getRawStoreXact(), segment, input_containerid, 
            template, columnOrder, collationIds, properties, 
            heap.getTypeFormatId(), 
            temporaryFlag);

		return heap;
	}

    /**
     * Return Conglomerate object for conglomerate with container_key.
     * <p>
     * Return the Conglomerate Object.  This is implementation specific.
     * Examples of what will be done is using the key to find the file where
     * the conglomerate is located, and then executing implementation specific
     * code to instantiate an object from reading a "special" row from a
     * known location in the file.  In the btree case the btree conglomerate
     * is stored as a column in the control row on the root page.
     * <p>
     * This operation is costly so it is likely an implementation using this
     * will cache the conglomerate row in memory so that subsequent accesses
     * need not perform this operation.
     *
     * @param xact_mgr      transaction to perform the create in.
     * @param container_key The unique id of the existing conglomerate.
     *
	 * @return An instance of the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Conglomerate readConglomerate(
    TransactionManager      xact_mgr,
    ContainerKey            container_key)
		throws StandardException
    {
        ContainerHandle         container   = null;
        Page                    page        = null;
        DataValueDescriptor[]   control_row = new DataValueDescriptor[1];

        try
        {
            // open container to read the Heap object out of it's control row.
            container = 
                xact_mgr.getRawStoreXact().openContainer(
                    container_key, (LockingPolicy) null, 0);

            if (container == null)
            {
                throw StandardException.newException(
                    SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST,
                    container_key.getContainerId());
            }

            // row in slot 0 of heap page 1 which is just a single column with
            // the heap entry.
            control_row[0]       = new Heap();

            page = container.getPage(ContainerHandle.FIRST_PAGE_NUMBER);

            RecordHandle rh = 
                page.fetchFromSlot(
                   (RecordHandle) null, 0, control_row, 
                   (FetchDescriptor) null,
                   true);

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(rh != null);

                // for now the control row is always the first id assigned on
                // page 1.
                SanityManager.ASSERT(rh.getId() == 6);
            }
        }
        finally
        {
            if (page != null)
                page.unlatch();

            if (container != null)
                container.close();
        }

        return((Conglomerate) control_row[0]);
    }

    /**
     * Interface to be called when an undo of an insert is processed.
     * <p>
     * Implementer of this class provides interface to be called by the raw
     * store when an undo of an insert is processed.  Initial implementation
     * will be by Access layer to queue space reclaiming events if necessary
     * when a rows is logically "deleted" as part of undo of the original
     * insert.  This undo can happen a lot for many applications if they
     * generate expected and handled duplicate key errors.
     * <p>
     * Caller may decide to call or not based on deleted row count of the
     * page, or if overflow rows/columns are present.
     *
     *
     * @param access_factory    current access_factory of the aborted insert.
     * @param xact              transaction that is being backed out.
     * @param page_key          page key of the aborted insert.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void insertUndoNotify(
    AccessFactory       access_factory,
    Transaction         xact,
    PageKey             page_key)
        throws StandardException
    {
        // try to reclaim rows when the page is only full of deleted rows,
        // or in the special case of the first page when all rows except the
        // "control row" are deleted.  Or if the row we just deleted is
        // a long row or has a long column.
        //
        // This logic is currently embedded in raw store InsertOperation
        // abort code which triggers the event to notify the 
        // HeapConglomerateFactory to post the HeapPostCommit work item.
        xact.addPostAbortWork(new HeapPostCommit(access_factory, page_key));
    }

	/*
	** Methods of ModuleControl.
	*/

	public boolean canSupport(Properties startParams) {

		String impl = 
            startParams.getProperty("derby.access.Conglomerate.type");

		if (impl == null)
			return false;

		return supportsImplementation(impl);
	}

	public void	boot(boolean create, Properties startParams)
		throws StandardException
	{
		// Find the UUID factory.
		UUIDFactory uuidFactory = 
            getMonitor().getUUIDFactory();
		
		// Make a UUID that identifies this conglomerate's format.
		formatUUID = uuidFactory.recreateUUID(FORMATUUIDSTRING);
	}

	public void	stop()
	{
	}

	/*
	** Methods of HeapConglomerateFactory
	*/

	public HeapConglomerateFactory()
	{
	}
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}

