/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.Conglomerate

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.ContainerKey;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.FormatableBitSet;


/**

A conglomerate is an abstract storage structure (they
correspond to access methods).  The Conglomerate interface
corresponds to a single instance of a conglomerate. In
other words, for each conglomerate in the system, there
will be one object implementing Conglomerate.
<P>
The Conglomerate interface is implemented by each access method.
The implementation must maintain enough information to properly
open the conglomerate and scans, and to drop the conglomerate.
This information typically will include the id of the container
or containers in which the conglomerate is stored, and my also
include property information.
<P>
Conglomerates are created by a conglomerate factory.  The access
manager stores them in a directory (which is why they implement
Storable).

**/

public interface Conglomerate extends Storable, DataValueDescriptor
{

    /**
     * Add a column to the conglomerate.
     * <p>
     * This routine update's the in-memory object version of the 
     * Conglomerate to have one more column of the type described by the
     * input template column.
     *
     * Note that not all conglomerates may support this feature.
     * 
	 * @param xact_manager     The TransactionController under which this 
     *                         operation takes place.
     * @param column_id        The column number to add this column at.
     * @param template_column  An instance of the column to be added to table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void addColumn(
    TransactionManager  xact_manager,
    int                 column_id,
    Storable            template_column)
        throws StandardException;

    /**
     * Drop this conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	void drop(TransactionManager  xact_manager)
		throws StandardException;

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
     * @param rawtran         The raw store xact to associate all ops with.
     *
	 * @param conglomId       The identifier of the conglomerate
	 *                        to open the scan for.
     *
	 * @param open_mode       Specifiy flags to control opening of table.  
     *                        OPENMODE_FORUPDATE - if set open the table for
     *                        update otherwise open table shared.
     * @param lock_level      One of (MODE_TABLE, MODE_RECORD, or MODE_NONE).
     *
     * @param locking_policy  The LockingPolicy to use to open the conglomerate.
     *
     * @param isolation_level The isolation level to lock the conglomerate at.
     *                        One of (ISOLATION_READ_COMMITTED, 
     *                        ISOLATION_REPEATABLE_READ, or 
     *                        ISOLATION_SERIALIZABLE).
     *
	 * @param scanColumnList  A description of which columns to return from 
     *                        every fetch in the scan. fetchRow  
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

	boolean fetchMaxOnBTree(
    TransactionManager      xact_manager,
    Transaction             rawtran,
    long                    conglomId,
    int                     open_mode,
    int                     lock_level,
    LockingPolicy           locking_policy,
    int                     isolation_level,
    FormatableBitSet                 scanColumnList,
    DataValueDescriptor[]   fetchRow)
        throws StandardException;


    /**
     * Get the containerid of conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * containerid.
     *
	 * @return The containerid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    long getContainerid();

    /**
     * Get the id of the container of the conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * container.  The ContainerKey is a combination of the container id
     * and segment id.
     *
	 * @return The ContainerKey.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    ContainerKey getId();

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
	 * @param tc        The TransactionController under which this operation 
     *                  takes place.
     * @param conglomId The identifier of the conglomerate to open.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
    TransactionController   tc,
    long                    conglomId)
		throws StandardException;

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
		throws StandardException;

    /**
     * Is this conglomerate temporary?
     * <p>
     *
	 * @return whether conglomerate is temporary or not.
     **/
    boolean isTemporary();

    /**
     * Bulk load into the conglomerate.
     * <p>
     * Individual rows that are loaded into the conglomerate are not
	 * logged. After this operation, the underlying database must be backed up
	 * with a database backup rather than an transaction log backup (when we 
     * have them). This warning is put here for the benefit of future 
     * generation.
     * <p>
	 * @param xact_manager  The TransactionController under which this operation
	 *                      takes place.
     *
	 * @param createConglom If true, the conglomerate is being created in the 
     *                      same operation as the openAndLoadConglomerate.  
     *                      The enables further optimization as recovery does
     *                      not require page allocation to be logged. 
     *
	 * @param rowSource     Where the rows come from.
     *
	 * @return The number of rows loaded.
     *
     * @exception StandardException Standard exception policy.  If 
     * conglomerage supports uniqueness checks and has been created to 
     * disallow duplicates, and one of the rows being loaded had key columns 
     * which were duplicate of a row already in the conglomerate, then 
     * raise SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION.
     *
     **/
	public long load(
	TransactionManager      xact_manager,
	boolean                 createConglom,
	RowLocationRetRowSource rowSource)
		 throws StandardException;


    /**
     * Open a conglomerate controller.
     * <p>
     *
	 * @return The open ConglomerateController.
     *
     * @param xact_manager   The access xact to associate all ops on cc with.
     * @param rawtran        The raw store xact to associate all ops on cc with.
     * @param open_mode      A bit mask of TransactionController.MODE_* bits,
     *                       indicating info about the open.
     * @param lock_level     Either TransactionController.MODE_TABLE or
     *                       TransactionController.MODE_RECORD, as passed into
     *                       the openConglomerate() call.
     * @param locking_policy The LockingPolicy to use to open the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see TransactionController
     **/
	ConglomerateController open(
    TransactionManager              xact_manager,
    Transaction                     rawtran, 
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    LockingPolicy                   locking_policy,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException;

    /**
     * Open a scan controller.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	ScanManager openScan(
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
    DataValueDescriptor[]           stopKeyValue,
    int                             stopSearchOperator,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
        throws StandardException;

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
    StoreCostController openStoreCost(
    TransactionManager  xact_manager,
    Transaction         rawtran)
		throws StandardException;

}
