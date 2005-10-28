/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapConglomerateFactory

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.UUID;

import java.util.Properties;

/**

  The heap conglomerate factory manages heap conglomerates implemented
  on the raw store.

**/

public class HeapConglomerateFactory implements ConglomerateFactory, ModuleControl, ModuleSupportable
{

	// RESOLVE (mikem) (STO062) 
    // The heap implementation id should be "heap table".
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
	ColumnOrdering[]        columnOrder,  //only meant for BTree type congloms
    Properties              properties,
	int                     temporaryFlag)
		throws StandardException
	{
		//parent.register(heap);
		Heap heap = new Heap();
		heap.create(
            xact_mgr.getRawStoreXact(), segment, input_containerid, 
            template, properties, temporaryFlag);

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
                    new Long(container_key.getContainerId()));
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
            Monitor.getMonitor().getUUIDFactory();
		
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
}

