/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IFactory

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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.ControlRow;

/**

  The "B2I" (acronym for b-tree secondary index) factory manages b-tree
  conglomerates implemented	on the raw store which are used as secondary
  indexes.
  <p>
  Most of this code is generic to all conglomerates.  This class might be
  more easily maintained as an abstract class in Raw/Conglomerate/Generic.
  The concrete ConglomerateFactories would simply have to supply the 
  IMPLEMENTATIONID, FORMATUUIDSTRING, and implement createConglomerate
  and defaultProperties.  Conglomerates which support more than one format
  would have to override supportsFormat, and conglomerates which support
  more than one implementation would have to override supportsImplementation.

**/

public class B2IFactory implements ConglomerateFactory, ModuleControl
{

	private static final String IMPLEMENTATIONID = "BTREE";
	private static final String FORMATUUIDSTRING = "C6CEEEF0-DAD3-11d0-BB01-0060973F0942";
	private UUID formatUUID;


	/*
	** Methods of MethodFactory (via ConglomerateFactory)
	*/

	/**
	Return the default properties for this kind of conglomerate.
	@see org.apache.derby.iapi.store.access.conglomerate.MethodFactory#defaultProperties
	**/
	public Properties defaultProperties()
	{
		// XXX (nat) Need to return the default b-tree secondary index properties.
		return new Properties();
	}

	/**
	Return whether this access method implements the implementation
	type given in the argument string.
	The btree only has one implementation type, "BTREE".

	@see org.apache.derby.iapi.store.access.conglomerate.MethodFactory#supportsImplementation
	**/
	public boolean supportsImplementation(String implementationId)
	{
		return implementationId.equals(IMPLEMENTATIONID);
	}

	/**
	Return the primary implementation type for this access method.
	The btree only has one implementation type, "BTREE".

	@see org.apache.derby.iapi.store.access.conglomerate.MethodFactory#primaryImplementationType
	**/
	public String primaryImplementationType()
	{
		return IMPLEMENTATIONID;
	}

	/**
	Return whether this access method supports the format supplied in
	the argument.
	The btree currently only supports one format.

	@see org.apache.derby.iapi.store.access.conglomerate.MethodFactory#supportsFormat
	**/
	public boolean supportsFormat(UUID formatid)
	{
		return formatid.equals(formatUUID);
	}

	/**
	Return the primary format that this access method supports.
	The btree currently only supports one format.

	@see org.apache.derby.iapi.store.access.conglomerate.MethodFactory#primaryFormat
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
     **/
    public int getConglomerateFactoryId()
    {
        return(ConglomerateFactory.BTREE_FACTORY_ID);
    }

	/**
	Create the conglomerate and return a conglomerate object for it.

	@see ConglomerateFactory#createConglomerate

    @exception StandardException Standard exception policy.
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
        B2I btree = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-2537

        if ((temporaryFlag & TransactionController.IS_TEMPORARY) != 0 &&
                xact_mgr.getAccessManager().isReadOnly())
        {
            // If this is a temporary conglomerate created for a read-only
            // database, we don't really care which disk format we use, since
            // it is not used for persisting data in the database. Use the
            // current format. A special case is needed because checkVersion()
            // throws an exception in read-only databases (DERBY-2354).
            btree = new B2I();
        }
        else if (xact_mgr.checkVersion(
                RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10,
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_4,
                null)) 
        {
            // on disk databases with version higher than 10.3 should use
            // current disk format B2I.  This includes new databases or
            // hard upgraded databases.
            btree = new B2I();
            
        }
        else if (xact_mgr.checkVersion(
                RawStoreFactory.DERBY_STORE_MAJOR_VERSION_10,
                RawStoreFactory.DERBY_STORE_MINOR_VERSION_3,
                null))
        {
            // Old databases that are running in new versions of the software,
            // but are running in soft upgrade mode at release level 10.3
            // use the 10.3 B2I version.  This version will
            // continue to write metadata that can be read by 10.3.
            btree = new B2I_10_3();
        }
        else
        {
            // Old databases that are running in new versions of the software,
            // but are running in soft upgrade mode at release level 10.2
            // and before should use the old B2I version.  This version will
            // continue to write metadata that can be read by 10.2 and previous
            // versions.
            btree = new B2I_v10_2();
        }

		btree.create(
            xact_mgr, segment, input_containerid, template, columnOrder, 
            collationIds, properties, temporaryFlag);

		return(btree);
	}

    /**
     * Return Conglomerate object for conglomerate with conglomid.
     * <p>
     * Return the Conglomerate Object.  This is implementation specific.
     * Examples of what will be done is using the id to find the file where
     * the conglomerate is located, and then executing implementation specific
     * code to instantiate an object from reading a "special" row from a
     * known location in the file.  In the btree case the btree conglomerate
     * is stored as a column in the control row on the root page.
     * <p>
     * This operation is costly so it is likely an implementation using this
     * will cache the conglomerate row in memory so that subsequent accesses
     * need not perform this operation.
     * <p>
     * The btree object returned by this routine may be installed in a cache
     * so the object must not change.
     *
	 * @return An instance of the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Conglomerate readConglomerate(
    TransactionManager  xact_manager,
    ContainerKey        container_key)
		throws StandardException
    {
        Conglomerate    btree      = null;
        ContainerHandle container  = null;
        ControlRow      root       = null;

        try
        {
            // open readonly, with no locks.  Dirty read is ok as it is the
            // responsibility of client code to make sure this data is not
            // changing while being read.  The only changes that currently
            // happen to this data is creation and deletion - no updates
            // ever happen to btree conglomerates.
            container = 
                (xact_manager.getRawStoreXact()).openContainer(
                    container_key,
                    (LockingPolicy) null,
                    ContainerHandle.MODE_READONLY);

            if (container == null)
            {
                // thrown a "known" error if the conglomerate does not exist 
                // which is checked for explicitly by callers of the store 
                // interface.

                throw StandardException.newException(
                    SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST, 
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                    container_key.getContainerId());
            }

            // The conglomerate is located in the control row on the root page.
            root = ControlRow.get(container, BTree.ROOTPAGEID);
//IC see: https://issues.apache.org/jira/browse/DERBY-2359

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(root.getPage().isLatched());

            // read the Conglomerate from it's entry in the control row.
            btree = (Conglomerate) root.getConglom(B2I.FORMAT_NUMBER);
//IC see: https://issues.apache.org/jira/browse/DERBY-5224

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(btree instanceof B2I);
        }
        finally
        {

            if (root != null)
                root.release();

            if (container != null)
                container.close();
        }

        // if any error, just return null - meaning can't access the container.

        return(btree);
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
        // Currently a no-op, btree's can reclaim space at split time. 
        // TODO - see if it makes sense to add postAbort action if the 
        // page has all deleted keys.  Shrinks don't work very well currently
        // as they require table level locks, so may not help much until that
        // issue is resolved.  see DERBY-5473

    }

	/*
	** Methods of ModuleControl.
	*/

	public boolean canSupport(Properties startParams) {

		String impl = startParams.getProperty("derby.access.Conglomerate.type");
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

		// Make a UUID that identifies this conglomerate's format.
		formatUUID = uuidFactory.recreateUUID(FORMATUUIDSTRING);
	}

	public void	stop()
	{
	}
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
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
