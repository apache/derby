/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory

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

package org.apache.derby.iapi.store.access.conglomerate;

import java.util.Properties;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.ColumnOrdering;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**

  The factory interface for all conglomerate access methods.

**/

public interface ConglomerateFactory extends MethodFactory
{

    static final int    HEAP_FACTORY_ID     = 0x00;
    static final int    BTREE_FACTORY_ID    = 0x01;


    /**
     * Return the conglomerate factory id.
     * <p>
     * Return a number in the range of 0-15 which identifies this factory.
     * Code which names conglomerates depends on this range currently, but
     * could be easily changed to handle larger ranges.   One hex digit seemed
     * reasonable for the number of conglomerate types currently implemented
     * (heap, btree) and those that might be implmented in the future: gist, 
     * gist btree, gist rtree, hash, others? ).
     * <p>
     *
	 * @return an unique identifier used to the factory into the conglomid.
     *
     **/
    int getConglomerateFactoryId();

	/**
	Create the conglomerate and return a conglomerate object
	for it.  It is expected that the caller of this method will place the
    the resulting object in the conglomerate directory.

    @param xact_mgr             transaction to perform the create in.
    @param segment              segment to create the conglomerate in.
    @param input_containerid    containerid to assign the container, or 
                                ContainerHandle.DEFAULT_ASSIGN_ID if you want
                                raw store to assign an id.
    @param template             Template of row in the conglomerate.
	@param columnOrder          columns sort order for Index creation
    @param properties           Properties associated with the conglomerate.

 	@exception StandardException if the conglomerate could not be
	opened for some reason, or if an error occurred in one of
	the lower level modules.
	**/
	Conglomerate createConglomerate(
    TransactionManager      xact_mgr,
    int                     segment,
    long                    input_containerid,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    Properties              properties,
	int						temporaryFlag)
            throws StandardException;
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
    Conglomerate readConglomerate(
    TransactionManager      xact_mgr,
    ContainerKey            container_key)
		throws StandardException;
}
