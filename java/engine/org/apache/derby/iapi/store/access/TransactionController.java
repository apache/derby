/*

   Derby - Class org.apache.derby.iapi.store.access.TransactionController

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

package org.apache.derby.iapi.store.access;

import java.util.Properties;

import java.io.Serializable;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.store.access.DatabaseInstant;
import org.apache.derby.iapi.error.ExceptionSeverity;
/**

The TransactionController interface provides methods that an access client
can use to control a transaction, which include the methods for
gaining access to resources (conglomerates, scans, etc.) in the transaction
controller's storage manager.  TransactionControllers are obtained
from an AccessFactory via the getTransaction method.
<P>
Each transaction controller is associated with a transaction context which
provides error cleanup when standard exceptions are thrown anywhere in the
system.  The transaction context performs the following actions in response
to cleanupOnError:
<UL>
<LI>
If the error is an instance of StandardException that has a severity less
than ExceptionSeverity.TRANSACTION_SEVERITY all resources remain unaffected.
<LI>
If the error is an instance of StandardException that has a severity equal
to ExceptionSeverity.TRANSACTION_SEVERITY, then all resources are released.  An attempt
to use any resource obtained from this transaction controller after
such an error will result in an error.  The transaction controller itself remains
valid, however.
<LI>
If the error is an instance of StandardException that has a severity greater
than ExceptionSeverity.TRANSACTION_SEVERITY, then all resources are released and the
context is popped from the stack.  Attempting to use this controller or any
resources obtained from it will result in an error.
</UL>
Transactions are obtained from an AccessFactory.
@see AccessFactory#getTransaction
@see org.apache.derby.iapi.error.StandardException
@see PersistentSet


**/

public interface TransactionController
    extends PersistentSet
{

    /**
     * Constant used for the lock_level argument to openConglomerate() and 
     * openScan() calls.  Pass in MODE_RECORD if you want the conglomerate
     * to be opened with record level locking (but the system may override
     * this choice and provide table level locking instead).  
     **/
	static final int MODE_RECORD    = 6;
    /**
     * Constant used for the lock_level argument to openConglomerate() and 
     * openScan() calls.  Pass in MODE_TABLE if you want the conglomerate
     * to be opened with table level locking - if this mode is passed in the
     * system will never use record level locking for the open scan or 
     * controller.
     **/
	static final int MODE_TABLE     = 7;

    /**
     * Constants used for the isolation_level argument to openConglomerate() and
     * openScan() calls.
     **/

    /**
     * 
     * No locks are requested for data that is read only.  Uncommitted data
     * may be returned.  Writes only visible previous to commit.
     * Exclusive transaction length locks are set on data that is written, no
     * lock is set on data that is read.  No table level intent lock is held
     * so it is up to caller to insure that table is not dropped while being
     * accessed (RESOLVE - this issue may need to be resolved differently if
     * we can't figure out a non-locked based way to prevent ddl during
     * read uncommitted access).
     *
     * ONLY USED INTERNALLY BY ACCESS, NOT VALID FOR EXTERNAL USERS.
     **/
	static final int ISOLATION_NOLOCK = 0;

    /**
     * No locks are requested for data that is read only.  Uncommitted data
     * may be returned.  Writes only visible previous to commit.
     * Exclusive transaction length locks are set on data that is written, no
     * lock is set on data that is read.  No table level intent lock is held
     * so it is up to caller to insure that table is not dropped while being
     * accessed (RESOLVE - this issue may need to be resolved differently if
     * we can't figure out a non-locked based way to prevent ddl during
     * read uncommitted access).
     *
     * Note that this is currently only supported in heap scans.
     *
     * TODO - work in progress to support this locking mode in the 5.1 
     * storage system.  
     **/
	static final int ISOLATION_READ_UNCOMMITTED = 1;

    /**
     * No lost updates, no dirty reads, only committed data is returned.  
     * Writes only visible when committed.  Exclusive transaction
     * length locks are set on data that is written, short term locks (
     * possibly instantaneous duration locks) are set
     * on data that is read.  
     **/
	static final int ISOLATION_READ_COMMITTED = 2;

    /**
     * No lost updates, no dirty reads, only committed data is returned.  
     * Writes only visible when committed.  Exclusive transaction
     * length locks are set on data that is written, short term locks (
     * possibly instantaneous duration locks) are set
     * on data that is read.  Read locks are requested for "zero" duration,
     * thus upon return from access no read row lock is held.
     **/
	static final int ISOLATION_READ_COMMITTED_NOHOLDLOCK = 3;

    /**
     * Read and write locks are held until end of transaction, but no
     * phantom protection is performed (ie. no previous key locking).
     * Writes only visible when committed. 
     *
     * Note this constant is currently mapped to ISOLATION_SERIALIZABLE.  
     * The constant is provided so that code which only requires repeatable 
     * read can be coded with the right isolation level, and will just work when
     * store provided real repeatable read isolation.
     **/
	static final int ISOLATION_REPEATABLE_READ = 4;

    /**
     * Gray's isolation degree 3, "Serializable, Repeatable Read".	Note that
     * some conglomerate implementations may only be able to provide 
     * phantom protection under MODE_TABLE, while others can support this
     * under MODE_RECORD.
     **/
	static final int ISOLATION_SERIALIZABLE = 5;

    /**
     * Constants used for the flag argument to openConglomerate() and 
     * openScan() calls.
     *
     * NOTE - The values of these constants must correspond to their associated
     * constants in 
     * protocol.Database.Storage.RawStore.Interface.ContainerHandle, do not
     * add constants to this file without first adding there.
     **/

    /**
     * Use this mode to the openScan() call to indicate the scan should get
     * update locks during scan, and either promote the update locks to 
     * exclusive locks if the row is changed or demote the lock if the row
     * is not updated.  The lock demotion depends on the isolation level of
     * the scan.  If isolation level is ISOLATION_SERIALIZABLE or 
     * ISOLATION_REPEATABLE_READ
     * then the lock will be converted to a read lock.  If the isolation level 
     * ISOLATION_READ_COMMITTED then the lock is released when the scan moves
     * off the row.
     * <p>
     * Note that one must still set OPENMODE_FORUPDATE to be able to change
     * rows in the scan.  So to enable update locks for an updating scan one
     * provides (OPENMODE_FORUPDATE | OPENMODE_USE_UPDATE_LOCKS)
     **/
    static final int OPENMODE_USE_UPDATE_LOCKS      = 0x00001000;

    /**
     * Use this mode to the openConglomerate() call which opens the base
     * table to be used in a index to base row probe.  This will cause
     * the openConglomerate() call to not get any row locks as part of
     * it's fetches.
     * It is important when using this mode that the secondary index table be
     * successfully opened before opening the base table so that
     * proper locking protocol is followed.
     **/
    static final int OPENMODE_SECONDARY_LOCKED      = 0x00002000;

    /**
     * Use this mode to the openConglomerate() call used to open the
     * secondary indices of a table for inserting new rows in the table.
     * This will let the secondaryindex know that the base row being inserted
     * has already been locked and only previous key locks need be obtained.
     * 
     * It is important when using this mode that the base table be
     * successfully opened before opening the secondaryindex so that
     * proper locking protocol is followed.
     **/
    static final int OPENMODE_BASEROW_INSERT_LOCKED = 0x00004000;

    /**
     * open table for update, if not specified table will be opened for read.
     **/
    static final int OPENMODE_FORUPDATE             = 0x00000004;

    /**
     * Use this mode to the openConglomerate() call used to just get the
     * table lock on the conglomerate without actually doing anything else.
     * Any operations other than close() performed on the "opened" container
     * will fail.
     **/
    static final int OPENMODE_FOR_LOCK_ONLY         = 0x00000040;

    /**
     * The table lock request will not wait.
     * <p>
     * The request to get the table lock (any table lock including intent or
     * "real" table level lock), will not wait if it can't be granted.   A
     * lock timeout will be returned.  Note that subsequent row locks will
     * wait if the application has not set a 0 timeout and if the call does
     * not have a wait parameter (like OpenConglomerate.fetch().
     **/
    static final int OPENMODE_LOCK_NOWAIT           = 0x00000080;

    /**
     * Constants used for the countOpen() call.
     **/
    public static final int OPEN_CONGLOMERATE   = 0x01;
    public static final int OPEN_SCAN           = 0x02;
    public static final int OPEN_CREATED_SORTS  = 0x03;
    public static final int OPEN_SORT           = 0x04;
    public static final int OPEN_TOTAL          = 0x05;


	static final byte IS_DEFAULT	=	(byte) 0x00; // initialize the flag
	static final byte IS_TEMPORARY	=	(byte) 0x01; // conglom is temporary
	static final byte IS_KEPT		=	(byte) 0x02; // no auto remove


    /**************************************************************************
     * Interfaces previously defined in TcAccessIface:
     **************************************************************************
     */

	/**
	Check whether a conglomerate exists.

	@param  conglomId  The identifier of the conglomerate to check for.

	@return  true if the conglomerate exists, false otherwise.

	@exception StandardException   only thrown if something goes
	wrong in the lower levels.
	**/
    boolean conglomerateExists(long conglomId)
		throws StandardException;

    /**
    Create a conglomerate.
	<p>
	Currently, only "heap"'s and ""btree secondary index"'s are supported, 
    and all the features are not completely implemented.  
    For now, create conglomerates like this:
	<p>
	<blockquote><pre>
		TransactionController tc;
		long conglomId = tc.createConglomerate(
			"heap", // we're requesting a heap conglomerate
			template, // a populated template is required for heap and btree.
			null, // default properties
			0); // not temporary
	</blockquote></pre>

    Each implementation of a conglomerate takes a possibly different set
    of properties.  The "heap" implementation currently takes no properties.

    The "btree secondary index" requires the following set of properties:
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
    <LI>"allowDuplicates" (boolean).  If set to true the table will allow 
    rows which are duplicate in key column's 0 through (nUniqueColumns - 1).
    Currently only supports "false".
    This property is optional, defaults to false.
    <LI>"nKeyFields"  (integer) Columns 0 through (nKeyFields - 1) will be 
    included in key of the conglomerate.
    This implementation requires that "nKeyFields" must be the same as the
    number of fields in the conglomerate, including the rowLocationColumn.
    Other implementations may relax this restriction to allow non-key fields
    in the index.
    This property is required.
    <LI>"nUniqueColumns" (integer) Columns 0 through "nUniqueColumns" will be 
    used to check for uniqueness.  So for a standard SQL non-unique index 
    implementation set "nUniqueColumns" to the same value as "nKeyFields"; and
    for a unique index set "nUniqueColumns" to "nKeyFields - 1 (ie. don't 
    include the rowLocationColumn in the uniqueness check).
    This property is required.
    <LI>"maintainParentLinks" (boolean)
    Whether the b-tree pages maintain the page number of their parent.  Only
    used for consistency checking.  It takes a certain amount more effort to
    maintain these links, but they're really handy for ensuring that the index
    is consistent.
    This property is optional, defaults to true.
    </UL>

    A secondary index i (a, b) on table t (a, b, c) would have rows
    which looked like (a, b, row_location).  baseConglomerateId is set to the
    conglomerate id of t.  rowLocationColumns is set to 2.  allowsDuplicates
    would be set to false.  To create a unique
    secondary index set uniquenessColumns to 2, this means that the btree
    code will compare the key values but not the row id when determing
    uniqueness.  To create a nonunique secondary index set uniquenessColumns
    to 3, this would mean that the uniqueness test would include the row
    location and since all row locations will be unique  all rows inserted
    into the index will be differentiated (at least) by row location.

	@return The identifier to be used to open the conglomerate later.

    @param implementation Specifies what kind of conglomerate to create.
	THE WAY THAT THE IMPLEMENTATION IS CHOSEN STILL NEEDS SOME WORK.
    For now, use "BTREE" or "heap" for a local access manager.

    @param template A row which describes the prototypical
	row that the conglomerate will be holding.
	Typically this row gives the conglomerate
	information about the number and type of
	columns it will be holding.  The implementation
	may require a specific subclass of row type.
    Note that the createConglomerate call reads the template and makes a copy
    of any necessary information from the template, no reference to the
    template is kept (and thus this template can be re-used in subsequent
    calls - such as openScan()).  This field is required when creating either
    a heap or btree conglomerate.

	@param columnOrder Specifies the colummns sort order.
	Useful only when the conglomerate is of type BTREE, default
	value is 'null', which means all columns needs to be sorted in 
	Ascending order.


	@param properties Implementation-specific properties of the
	conglomerate.  

    @param  temporaryFlag
	Where temporaryFlag can have the following values:
	IS_DEFAULT		- no bit is set.
    IS_TEMPORARY	- if set, the conglomerate is temporary
    IS_KEPT			- only looked at if IS_TEMPORARY,
					  if set, the temporary container is not
					  removed automatically by store when
					  transaction terminates.

	If IS_TEMPORARY is set, the conglomerate is temporary.
	Temporary conglomerates are only visible through the transaction
	controller that created them.  Otherwise, they are opened,
	scanned, and dropped in the same way as permanent conglomerates.
	Changes to temporary conglomerates persist across commits, but
	temporary conglomerates are truncated on abort (or rollback
	to savepoint).  Updates to temporary conglomerates are not 
	locked or logged.

	A temporary conglomerate is only visible to the	transaction
	controller that created it, even if the conglomerate IS_KEPT
	when the transaction termination.

	All temporary conglomerate is removed by store when the
	conglomerate controller is destroyed, or if it is dropped by an explicit
	dropConglomerate.  If cloudscape reboots, all temporary
	conglomerates are removed.

	@exception  StandardException  if the conglomerate could
	not be created for some reason.
    **/
    long createConglomerate(
    String                  implementation,
    DataValueDescriptor[]   template,
    ColumnOrdering[]        columnOrder,
    Properties              properties,
    int                     temporaryFlag)
		throws StandardException;

	/**
	Create a conglomerate and load (filled) it with rows that comes from the
	row source without loggging.  

 	<p>Individual rows that are loaded into the conglomerate are not
 	logged. After this operation, the underlying database must be backed up
 	with a database backup rather than an transaction log backup (when we have
 	them). This warning is put here for the benefit of future generation.

	<p>
	This function behaves the same as @see createConglomerate except it also
	populates the conglomerate with rows from the row source and the rows that
	are inserted are not logged.

    @param implementation Specifies what kind of conglomerate to create.
	THE WAY THAT THE IMPLEMENTATION IS CHOSEN STILL NEEDS SOME WORK.
    For now, use "BTREE" or "heap" for a local access manager.

	@param rowSource the interface to recieve rows to load into the
	conglomerate. 

	@param rowCount - if not null the number of rows loaded into the table
	will be returned as the first element of the array.

	@exception StandardException if the conglomerate could not be created or
	loaded for some reason.  Throws 
    SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION if
	the conglomerate supports uniqueness checks and has been created to
	disallow duplicates, and one of the rows being loaded had key columns which
	were duplicate of a row already in the conglomerate.
	**/
    long createAndLoadConglomerate(
    String                  implementation,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    Properties              properties,
    int                     temporaryFlag,
    RowLocationRetRowSource rowSource,
	long[] rowCount)
    throws StandardException;

	/**
    Recreate a conglomerate and possibly load it with new rows that come from
    the new row source.

	<p>
	This function behaves the same as @see createConglomerate except it also
	populates the conglomerate with rows from the row source and the rows that
	are inserted are not logged.

	<p>Individual rows that are loaded into the conglomerate are not
	logged. After this operation, the underlying database must be backed up
	with a database backup rather than an transaction log backup (when we have
	them). This warning is put here for the benefit of future generation.

    @param implementation Specifies what kind of conglomerate to create.
	THE WAY THAT THE IMPLEMENTATION IS CHOSEN STILL NEEDS SOME WORK.
    For now, use "BTREE" or "heap" for a local access manager.

    @param recreate_ifempty If false, and the rowsource used to load the new
                            conglomerate returns no rows, then the original
                            conglomid will be returned.  To the client it will
                            be as if no call was made.  Underlying 
                            implementations may actually create and drop a 
                            container.
                            If true, then a new empty container will be 
                            created and it's conglomid will be returned.

    @param template A row which describes the prototypical
	row that the conglomerate will be holding.
	Typically this row gives the conglomerate
	information about the number and type of
	columns it will be holding.  The implementation
	may require a specific subclass of row type.
    Note that the createConglomerate call reads the template and makes a copy
    of any necessary information from the template, no reference to the
    template is kept (and thus this template can be re-used in subsequent
    calls - such as openScan()).  This field is required when creating either
    a heap or btree conglomerate.

	@param columnOrder  Specifies the colummns sort order.
	Useful only when the conglomerate is of type BTREE, default
	value is 'null', which means all columns needs to be sorted in 
	Ascending order.

	@param properties Implementation-specific properties of the conglomerate.  

    @param  temporary  If true, the conglomerate is temporary.
	Temporary conglomerates are only visible through the transaction
	controller that created them.  Otherwise, they are opened,
	scanned, and dropped in the same way as permanent conglomerates.
	Changes to temporary conglomerates persist across commits, but
	temporary conglomerates are truncated on abort (or rollback
	to savepoint).  Updates to temporary conglomerates are not 
	locked or logged.

	@param orig_conglomId The conglomid of the original conglomerate.

	@param rowSource interface to receive rows to load into the conglomerate. 

	@param rowCount - if not null the number of rows loaded into the table
	will be returned as the first element of the array.

    @exception StandardException if the conglomerate could not be created or
	loaded for some reason.  Throws 
    SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION if
	the conglomerate supports uniqueness checks and has been created to
	disallow duplicates, and one of the rows being loaded had key columns which
	were duplicate of a row already in the conglomerate.
	**/
    long recreateAndLoadConglomerate(
    String                  implementation,
    boolean                 recreate_ifempty,
    DataValueDescriptor[]   template,
	ColumnOrdering[]		columnOrder,
    Properties              properties,
    int			            temporaryFlag,
    long                    orig_conglomId,
    RowLocationRetRowSource rowSource,
	long[] rowCount
	)
        throws StandardException;

    /**
    Add a column to a conglomerate.  
    
    The Storage system will block this action until it can get an exclusive
    container level lock on the conglomerate.  The conglomerate must not be
    open in the current transaction, this means that within the current 
    transaction there must be no open ConglomerateController's or 
    ScanControllers.  It may not be possible in some implementations of the
    system to catch this error in the store, so it is up to the caller to 
    insure this.

    The column can only be added at the spot just after the current set of
    columns.  

    The template_column must be nullable.  
    
    After this call has been made, all fetches of this column from rows that
    existed in the table prior to this call will return "null".

	@param conglomId        The identifier of the conglomerate to alter.
	@param column_id        The column number to add this column at.
	@param template_column  An instance of the column to be added to table.

	@exception StandardException Only some types of conglomerates can support
        adding a column, for instance "heap" conglomerates support adding a 
        column while "btree" conglomerates do not.  If the column can not be
        added an exception will be thrown.
    **/
    public void addColumnToConglomerate(
    long        conglomId, 
    int         column_id, 
    Storable    template_column)
		throws StandardException;


    /**
    Drop a conglomerate.  The conglomerate must not be open in
	the current transaction.  This also means that there must
	not be any active scans on it.

	@param conglomId The identifier of the conglomerate to drop.

	@exception StandardException if the conglomerate could not be
	 dropped for some reason.
    **/
    void dropConglomerate(long conglomId)
		throws StandardException;

    /**
     * For debugging, find the conglomid given the containerid.
     * <p>
     *
	 * @return the conglomid, which contains the container with containerid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    long findConglomid(long containerid)
		throws StandardException;

    /**
     * For debugging, find the containerid given the conglomid.
     * <p>
     * Will have to change if we ever have more than one container in 
     * a conglomerate.
     *
	 * @return the containerid of container implementing conglomerate with 
     *             "conglomid."
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    long findContainerid(long conglomid)
		throws StandardException;

    /**
     * Get an nested user transaction.
     * <p>
     * A nested user transaction can be used exactly as any other 
     * TransactionController, except as follows.  For this discussion let the 
     * parent transaction be the transaction used to make the 
     * startNestedUserTransaction() call, and let the child transaction be the
     * transaction returned by the startNestedUserTransaction() call.
     * <p>
     * Only 1 non-readOnly nested user transaction can exist.  If a subsequent
     * non-readOnly transaction creation is attempted prior to destroying an
     * existing write nested user transaction an exception will be thrown.  
     * <p>
     * The nesting is limited to one level deep.  An exception will be thrown
     * if a subsequent getNestedUserTransaction() is called on the child
     * transaction.
     * <p>
     * The locks in the child transaction of a readOnly nested user transaction
     * will be compatible with the locks of the parent transaction.  The
     * locks in the child transaction of a non-readOnly nested user transaction
     * will NOT be compatible with those of the parent transaction - this is
     * necessary for correct recovery behavior.
     * <p>
     * A commit in the child transaction will release locks associated with
     * the child transaction only, work can continue in the parent transaction
     * at this point.  
     * <p>
     * Any abort of the child transaction will result in an abort of both
     * the child transaction and parent transaction, either initiated by
     * an explict abort() call or by an exception that results in an abort.
     * <p>
     * A TransactionController.destroy() call should be made on the child
     * transaction once all child work is done, and the caller wishes to 
     * continue work in the parent transaction.
     * <p>
     * AccessFactory.getTransaction() will always return the "parent" 
     * transaction, never the child transaction.  Thus clients using 
     * nested user transactions must keep track of the transaction, as there
     * is no interface to query the storage system to get the current
     * child transaction.  The idea is that a nested user transaction should
     * be used to for a limited amount of work, committed, and then work
     * continues in the parent transaction.
     * <p>
     * Nested User transactions are meant to be used to implement 
     * system work necessary to commit as part of implementing a user's
     * request, but where holding the lock for the duration of the user
     * transaction is not acceptable.  2 examples of this are system catalog
     * read locks accumulated while compiling a plan, and auto-increment.
     * <p>
     * Once the first write of a non-readOnly nested transaction is done,
     * then the nested user transaction must be committed or aborted before
     * any write operation is attempted in the parent transaction.  
     *
     * @param readOnly  Is transaction readonly?  Only 1 non-readonly nested
     *                  transaction is allowed per transaction.
     *
	 * @return The new nested user transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public TransactionController startNestedUserTransaction(boolean readOnly)
        throws StandardException;

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
    Properties getUserCreateConglomPropList();

    /**
     * Open a conglomerate for use.  
     * <p>
     * The lock level indicates the minimum lock level to get locks at, the
     * underlying conglomerate implementation may actually lock at a higher
     * level (ie. caller may request MODE_RECORD, but the table may be locked
     * at MODE_TABLE instead).
     * <p>
     * The close method is on the ConglomerateController interface.
     *
	 * @return a ConglomerateController to manipulate the conglomerate.
     *
     * @param conglomId         The identifier of the conglomerate to open.
     *
	 * @param hold              If true, will be maintained open over commits.
     *
	 * @param open_mode         Specifiy flags to control opening of table.  
     *                          OPENMODE_FORUPDATE - if set open the table for
     *                          update otherwise open table shared.
     *
     * @param lock_level        One of (MODE_TABLE, MODE_RECORD).
     *
     * @param isolation_level   The isolation level to lock the conglomerate at.
     *                          One of (ISOLATION_READ_COMMITTED, 
     *                          ISOLATION_REPEATABLE_READ or 
     *                          ISOLATION_SERIALIZABLE).
     *
	 * @exception  StandardException  if the conglomerate could not be opened 
     *                                for some reason.  Throws 
     *                                SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST
     *                                if the conglomId being requested does not
     *                                exist for some reason (ie. someone has 
     *                                dropped it).
     **/
    ConglomerateController openConglomerate(
    long                            conglomId, 
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level)
		throws StandardException;

    /**
     * Open a conglomerate for use, optionally include "compiled" info.  
     * <p>
     * Same as openConglomerate(), except that one can optionally provide
     * "compiled" static_info and/or dynamic_info.  This compiled information
     * must have be gotten from getDynamicCompiledConglomInfo() and/or
     * getStaticCompiledConglomInfo() calls on the same conglomid being opened.
     * It is up to caller that "compiled" information is still valid and
     * is appropriately multi-threaded protected.
     * <p>
     *
     * @see TransactionController#openConglomerate
     * @see TransactionController#getDynamicCompiledConglomInfo
     * @see TransactionController#getStaticCompiledConglomInfo
     * @see DynamicCompiledOpenConglomInfo
     * @see StaticCompiledOpenConglomInfo
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
	 * @param hold              If true, will be maintained open over commits.
	 * @param open_mode         Specifiy flags to control opening of table.  
     * @param lock_level        One of (MODE_TABLE, MODE_RECORD).
     * @param isolation_level   The isolation level to lock the conglomerate at.
     *                          One of (ISOLATION_READ_COMMITTED, 
     *                          ISOLATION_REPEATABLE_READ or 
     *                          ISOLATION_SERIALIZABLE).
     * @param static_info       object returned from 
     *                          getStaticCompiledConglomInfo() call on this id.
     * @param dynamic_info      object returned from
     *                          getDynamicCompiledConglomInfo() call on this id.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    ConglomerateController openCompiledConglomerate(
    boolean                         hold,
    int                             open_mode,
    int                             lock_level,
    int                             isolation_level,
    StaticCompiledOpenConglomInfo   static_info,
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException;


    /**
     * Create a HashSet which contains all rows that qualify for the 
     * described scan.
     * <p>
     * All parameters shared between openScan() and this routine are 
     * interpreted exactly the same.  Logically this routine calls
     * openScan() with the passed in set of parameters, and then places
     * all returned rows into a newly created HashSet and returns, actual
     * implementations will likely perform better than actually calling
     * openScan() and doing this.  For documentation of the openScan 
     * parameters see openScan().
     * <p>
     *
	 * @return the BackingStoreHashtable which was created.
     *
	 * @param conglomId             see openScan()
     * @param open_mode             see openScan()
     * @param lock_level            see openScan()
     * @param isolation_level       see openScan()
     * @param scanColumnList        see openScan()
     * @param startKeyValue         see openScan()
     * @param startSearchOperator   see openScan()
     * @param qualifier[]           see openScan()
     * @param stopKeyValue          see openScan()
     * @param stopSearchOperator    see openScan()
     *
     * @param max_rowcnt            The maximum number of rows to insert into 
     *                              the HashSet.  Pass in -1 if there is no 
     *                              maximum.
     * @param key_column_numbers    The column numbers of the columns in the
     *                              scan result row to be the key to the 
     *                              Hashtable.  "0" is the first column in the 
     *                              scan result row (which may be different 
     *                              than the first row in the table of the 
     *                              scan).
     * @param remove_duplicates     Should the HashSet automatically remove
     *                              duplicates, or should it create the Vector 
     *                              of duplicates?
     * @param estimated_rowcnt      The number of rows that the caller 
     *                              estimates will be inserted into the sort. 
     *                              -1 indicates that the caller has no idea.
     *                              Used by the sort to make good choices about
     *                              in-memory vs. external sorting, and to size
     *                              merge runs.
     * @param max_inmemory_rowcnt   The number of rows at which the underlying
     *                              Hashtable implementation should cut over
     *                              from an in-memory hash to a disk based
     *                              access method.
     * @param initialCapacity       If not "-1" used to initialize the java
     *                              Hashtable.
     * @param loadFactor            If not "-1" used to initialize the java
     *                              Hashtable.
     * @param collect_runtimestats  If true will collect up runtime stats during
     *                              scan processing for retrieval by
     *                              BackingStoreHashtable.getRuntimeStats().
	 * @param skipNullKeyColumns	Whether or not to skip rows with 1 or more null key columns
     *
     * @see BackingStoreHashtable
     * @see TransactionController#openScan
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    BackingStoreHashtable createBackingStoreHashtableFromScan(
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
    boolean		            skipNullKeyColumns)
        throws StandardException;


	/**
	Open a scan on a conglomerate.  The scan will return all
	rows in the conglomerate which are between the
	positions defined by {startKeyValue, startSearchOperator} and
	{stopKeyValue, stopSearchOperator}, which also match the qualifier.
	<P>
	The way that starting and stopping keys and operators are used
	may best be described by example. Say there's an ordered conglomerate
	with two columns, where the 0-th column is named 'x', and the 1st
	column is named 'y'.  The values of the columns are as follows:
	<blockquote><pre>
	  x: 1 3 4 4 4 5 5 5 6 7 9
	  y: 1 1 2 4 6 2 4 6 1 1 1
	</blockquote></pre>
	<P>
	A {start key, search op} pair of {{5.2}, GE} would position on
	{x=5, y=2}, whereas the pair {{5}, GT} would position on {x=6, y=1}.
	<P>
	Partial keys are used to implement partial key scans in SQL.
	For example, the SQL "select * from t where x = 5" would
	open a scan on the conglomerate (or a useful index) of t
	using a starting position partial key of {{5}, GE} and
	a stopping position partial key of {{5}, GT}.
	<P>
	Some more examples:
	<p>
	<blockquote><pre>
	+-------------------+------------+-----------+--------------+--------------+
	| predicate         | start key  | stop key  | rows         | rows locked  |
	|                   | value | op | value |op | returned     |serialization |
	+-------------------+-------+----+-------+---+--------------+--------------+
	| x = 5             | {5}   | GE | {5}   |GT |{5,2} .. {5,6}|{4,6} .. {5,6}|
	| x > 5             | {5}   | GT | null  |   |{6,1} .. {9,1}|{5,6} .. {9,1}|
	| x >= 5            | {5}   | GE | null  |   |{5,2} .. {9,1}|{4,6} .. {9,1}|
	| x <= 5            | null  |    | {5}   |GT |{1,1} .. {5,6}|first .. {5,6}|
  	| x < 5             | null  |    | {5}   |GE |{1,1} .. {4,6}|first .. {4,6}|
	| x >= 5 and x <= 7 | {5},  | GE | {7}   |GT |{5,2} .. {7,1}|{4,6} .. {7,1}|
	| x = 5  and y > 2  | {5,2} | GT | {5}   |GT |{5,4} .. {5,6}|{5,2} .. {5,6}|
	| x = 5  and y >= 2 | {5,2} | GE | {5}   |GT |{5,2} .. {5,6}|{4,6} .. {5,6}|
	| x = 5  and y < 5  | {5}   | GE | {5,5} |GE |{5,2} .. {5,4}|{4,6} .. {5,4}|
	| x = 2             | {2}   | GE | {2}   |GT | none         |{1,1} .. {1,1}|
	+-------------------+-------+----+-------+---+--------------+--------------+
	</blockquote></pre>
	<P>
	As the above table implies, the underlying scan may lock
	more rows than it returns in order to guarantee serialization.
    <P>
    For each row which meets the start and stop position, as described above
    the row is "qualified" to see whether it should be returned.  The
    qualification is a 2 dimensional array of @see Qualifiers, which represents
    the qualification in conjunctive normal form (CNF).  Conjunctive normal
    form is an "and'd" set of "or'd" Qualifiers.
    <P>
    For example x = 5 would be represented is pseudo code as:
    
    qualifier_cnf[][] = new Qualifier[1];
    qualifier_cnf[0]  = new Qualifier[1];

    qualifier_cnr[0][0] = new Qualifer(x = 5)

    <P>
    For example (x = 5) or (y = 6) would be represented is pseudo code as:

    qualifier_cnf[][] = new Qualifier[1];
    qualifier_cnf[0]  = new Qualifier[2];

    qualifier_cnr[0][0] = new Qualifer(x = 5)
    qualifier_cnr[0][1] = new Qualifer(y = 6)

    <P>
    For example ((x = 5) or (x = 6)) and ((y = 1) or (y = 2)) would be 
    represented is pseudo code as:

    qualifier_cnf[][] = new Qualifier[2];
    qualifier_cnf[0]  = new Qualifier[2];

    qualifier_cnr[0][0] = new Qualifer(x = 5)
    qualifier_cnr[0][1] = new Qualifer(x = 6)

    qualifier_cnr[0][0] = new Qualifer(y = 5)
    qualifier_cnr[0][1] = new Qualifer(y = 6)

    <P>
    For each row the CNF qualfier is processed and it is determined whether
    or not the row should be returned to the caller.

    The following pseudo-code describes how this is done:

    <blockquote><pre>
    if (qualifier != null)
    {
        <blockquote><pre>
		for (int and_clause; and_clause < qualifier.length; and_clause++)
		{
            boolean or_qualifies = false;

            for (int or_clause; or_clause < qualifier[and_clause].length; or_clause++)
            {
                <blockquote><pre>
                DataValueDescriptor key     = 
                    qualifier[and_clause][or_clause].getOrderable();

                DataValueDescriptor row_col = 
                    get row column[qualifier[and_clause][or_clause].getColumnId()];

                boolean or_qualifies = 
                row_col.compare(qualifier[i].getOperator,
                <blockquote><pre>
                key,
                qualifier[i].getOrderedNulls,
                qualifier[i].getUnknownRV);
                </blockquote></pre>

                if (or_qualifies)
                {
                    break;
                }
            }

            if (!or_qualifies)
            {
                <blockquote><pre>
                don't return this row to the client - proceed to next row;
                </blockquote></pre>
            }
            </blockquote></pre>

        }
        </blockquote></pre>
    }
    </blockquote></pre>


	@param conglomId The identifier of the conglomerate
	to open the scan for.

	@param hold If true, this scan will be maintained open over
	commits.

	@param open_mode         Specifiy flags to control opening of table.  
                             OPENMODE_FORUPDATE - if set open the table for
                             update otherwise open table shared.

    @param lock_level        One of (MODE_TABLE, MODE_RECORD).

    @param isolation_level   The isolation level to lock the conglomerate at.
                             One of (ISOLATION_READ_COMMITTED, 
                             ISOLATION_REPEATABLE_READ or 
                             ISOLATION_SERIALIZABLE).

    @param isolation_level   The isolation level to lock the conglomerate at.
                             One of (ISOLATION_READ_COMMITTED, 
                             ISOLATION_REPEATABLE_READ or 
                             ISOLATION_SERIALIZABLE).

	@param scanColumnList A description of which columns to return from 
    every fetch in the scan.  template, and scanColumnList
    work together to describe the row to be returned by the scan - see RowUtil
    for description of how these three parameters work together to describe
    a "row".

	@param startKeyValue  An indexable row which holds a 
	(partial) key value which, in combination with the
	startSearchOperator, defines the starting position of
	the scan.  If null, the starting position of the scan
	is the first row of the conglomerate.
    The startKeyValue must only reference columns included
    in the scanColumnList.
	
	@param startSearchOperation an operator which defines
	how the startKeyValue is to be searched for.  If 
    startSearchOperation is ScanController.GE, the scan starts on
	the first row which is greater than or equal to the 
	startKeyValue.  If startSearchOperation is ScanController.GT,
	the scan starts on the first row whose key is greater than
	startKeyValue.  The startSearchOperation parameter is 
	ignored if the startKeyValue parameter is null.

	@param qualifier A 2 dimensional array encoding a conjunctive normal
    form (CNF) datastructure of of qualifiers which, applied
	to each key, restrict the rows returned by the scan.  Rows
	for which the CNF expression returns false are not
	returned by the scan. If null, all rows are returned.
    Qualifiers can only reference columns which are included in the
    scanColumnList.  The column id that a qualifier returns is the
    column id the table, not the column id in the partial row being
    returned.

    For detailed description of 2-dimensional array passing @see Qualifier

	@param stopKeyValue  An indexable row which holds a 
	(partial) key value which, in combination with the
	stopSearchOperator, defines the ending position of
	the scan.  If null, the ending position of the scan
	is the last row of the conglomerate.
    The stopKeyValue must only reference columns included
    in the scanColumnList.
	
	@param stopSearchOperation an operator which defines
	how the stopKeyValue is used to determine the scan stopping
	position. If stopSearchOperation is ScanController.GE, the scan 
	stops just before the first row which is greater than or
	equal to the stopKeyValue.  If stopSearchOperation is
	ScanController.GT, the scan stops just before the first row whose
	key is greater than	startKeyValue.  The stopSearchOperation
	parameter is ignored if the stopKeyValue parameter is null.

 	@exception StandardException if the scan could not be
	opened for some reason.  Throws SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST
    if the conglomId being requested does not exist for some reason (ie. 
    someone has dropped it).

    @see RowUtil
    @see ScanController
	**/
	ScanController openScan(
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
			throws StandardException;


    /**
     * Open a scan on a conglomerate, optionally providing compiled info.
     * <p>
     * Same as openScan(), except that one can optionally provide
     * "compiled" static_info and/or dynamic_info.  This compiled information
     * must have be gotten from getDynamicCompiledConglomInfo() and/or
     * getStaticCompiledConglomInfo() calls on the same conglomid being opened.
     * It is up to caller that "compiled" information is still valid and
     * is appropriately multi-threaded protected.
     * <p>
     *
     * @see TransactionController#openScan
     * @see TransactionController#getDynamicCompiledConglomInfo
     * @see TransactionController#getStaticCompiledConglomInfo
     * @see DynamicCompiledOpenConglomInfo
     * @see StaticCompiledOpenConglomInfo
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
	 * @param conglomId             see openScan()
     * @param open_mode             see openScan()
     * @param lock_level            see openScan()
     * @param isolation_level       see openScan()
     * @param scanColumnList        see openScan()
     * @param startKeyValue         see openScan()
     * @param startSearchOperator   see openScan()
     * @param qualifier[]           see openScan()
     * @param stopKeyValue          see openScan()
     * @param stopSearchOperator    see openScan()
     * @param static_info       object returned from 
     *                          getStaticCompiledConglomInfo() call on this id.
     * @param dynamic_info      object returned from
     *                          getDynamicCompiledConglomInfo() call on this id.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	ScanController openCompiledScan(
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
			throws StandardException;


    /**
     * Open a scan which gets copies of multiple rows at a time.
     * <p>
     * All inputs work exactly as in openScan().  The return is 
     * a GroupFetchScanController, which only allows fetches of groups
     * of rows from the conglomerate.
     * <p>
     *
	 * @return The GroupFetchScanController to be used to fetch the rows.
     *
	 * @param conglomId             see openScan()
     * @param open_mode             see openScan()
     * @param lock_level            see openScan()
     * @param isolation_level       see openScan()
     * @param scanColumnList        see openScan()
     * @param startKeyValue         see openScan()
     * @param startSearchOperator   see openScan()
     * @param qualifier[]           see openScan()
     * @param stopKeyValue          see openScan()
     * @param stopSearchOperator    see openScan()
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see ScanController
     * @see GroupFetchScanController
     **/
	GroupFetchScanController openGroupFetchScan(
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
			throws StandardException;


    /**
     * Retrieve the maximum value row in an ordered conglomerate.
     * <p>
     * Returns true and fetches the rightmost non-null row of an ordered 
     * conglomerate into "fetchRow" if there is at least one non-null row in 
     * the conglomerate.  If there are no non-null rows in the conglomerate it 
     * returns false.  Any row with
     * a first column with a Null is considered a "null" row.
     * <p>
     * Non-ordered conglomerates will not implement this interface, calls
     * will generate a StandardException.
     * <p>
     * RESOLVE - this interface is temporary, long term equivalent (and more) 
     * functionality will be provided by the openBackwardScan() interface.  
     * <p>
     * ISOLATION_SERIALIZABLE and MODE_RECORD locking for btree max:
     * The "BTREE" implementation will at the very least get a shared row lock
     * on the max key row and the key previous to the max.  
     * This will be the case where the max row exists in the rightmost page of
     * the btree.  These locks won't be released.  If the row does not exist in
     * the last page of the btree then a scan of the entire btree will be
     * performed, locks acquired in this scan will not be released.
     * <p>
     * Note that under ISOLATION_READ_COMMITTED, all locks on the table
     * are released before returning from this call.
     *
	 * @param conglomId       The identifier of the conglomerate
	 *                        to open the scan for.
     *
	 * @param open_mode       Specifiy flags to control opening of table.  
     *                        OPENMODE_FORUPDATE - if set open the table for
     *                        update otherwise open table shared.
     * @param lock_level      One of (MODE_TABLE, MODE_RECORD).
     *
     * @param isolation_level   The isolation level to lock the conglomerate at.
     *                          One of (ISOLATION_READ_COMMITTED, 
     *                          ISOLATION_REPEATABLE_READ or 
     *                          ISOLATION_SERIALIZABLE).
     *
	 * @param scanColumnList  A description of which columns to return from 
     *                        every fetch in the scan.  template, and 
     *                        scanColumnList work together
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
	boolean fetchMaxOnBtree(
		long                    conglomId,
		int                     open_mode,
        int                     lock_level,
        int                     isolation_level,
		FormatableBitSet                 scanColumnList,
		DataValueDescriptor[]   fetchRow)
			throws StandardException;


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
    StoreCostController openStoreCost(
    long        conglomId)
		throws StandardException;


    /**
     * Report on the number of open conglomerates in the transaction.
     * <p>
     * There are 4 types of open "conglomerates" that can be tracked, those
     * opened by each of the following: openConglomerate(), openScan(), 
     * createSort(),  and openSort().  Scans opened by openSortScan() are 
     * tracked the same as those opened by openScan().  This routine can be
     * used to either report on the number of all opens, or may be used to
     * track one particular type of open.
     * <p>
     * This routine is expected to be used for debugging only.  An 
     * implementation may only track this info under SanityManager.DEBUG mode.
     * If the implementation does not track the info it will return -1 (so
     * code using this call to verify that no congloms are open should check
     * for return <= 0 rather than == 0).
     * <p>
     * The return value depends on the "which_to_count" parameter as follows:
     * <UL>
     * <LI>
     * OPEN_CONGLOMERATE  - return # of openConglomerate() calls not close()'d.
     * <LI>
     * OPEN_SCAN          - return # of openScan() + openSortScan() calls not
     *                      close()'d.
     * <LI>
     * OPEN_CREATED_SORTS - return # of sorts created (createSort()) in 
     *                      current xact.  There is currently no way to get
     *                      rid of these sorts before end of transaction.
     * <LI>
     * OPEN_SORT          - return # of openSort() calls not close()'d.
     * <LI>
     * OPEN_TOTAL         - return total # of all above calls not close()'d.
     * </UL>
     *     - note an implementation may return -1 if it does not track the
     *       above information.
     * <p>
	 * @return The nunber of open's of a type indicated by "which_to_count"
     *         parameter.
     *
     * @param which_to_count Which kind of open to report on.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int countOpens(int which_to_count)
		throws StandardException;


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
    public String debugOpened() throws StandardException;


	/**
		Get an object to handle non-transactional files.
	*/
	public FileResource getFileHandler();

	/**
		Return an object that when used as the compatability space *and*
		group for a lock request, guarantees that the lock will be removed
		on a commit or an abort.
	*/
	public Object getLockObject();

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

    /**************************************************************************
     * Interfaces previously defined in TcCacheStatIface:
     **************************************************************************
     */

	/**
		Get cache statistics for the specified cache
	*/
	public long[] getCacheStats(String cacheName);

	/**
		Reset the cache statistics for the specified cache
	*/
	public void resetCacheStats(String cacheName);


    /**************************************************************************
     * Interfaces previously defined in TcLogIface:
     **************************************************************************
     */
	/**
    Log an operation and then action it in the context of this
    transaction.

    <P>This simply passes the operation to the RawStore which logs and
    does it.
    

    @param operation the operation that is to be applied

    @see org.apache.derby.iapi.store.raw.Loggable
    @see org.apache.derby.iapi.store.raw.Transaction#logAndDo
    @exception StandardException  Standard cloudscape exception policy
	**/
	public void logAndDo(Loggable operation) throws StandardException;


    /**************************************************************************
     * Interfaces previously defined in TcSortIface:
     **************************************************************************
     */

	/**
	Create a sort.  Rows are inserted into the sort with a
	sort controller, and subsequently retrieved with a
	sort scan controller. The rows come out in the order
	specified by the parameters.
	<p>
	Sorts also do aggregation. The input (unaggregated) rows
	have the same format as	the aggregated rows, and the
	aggregate results are part of the both rows.  The sorter,
	when it notices that a row is a duplicate of another,
	calls a user-supplied aggregation method (see interface
	Aggregator), passing it both rows.  One row is known as
	the 'addend' and the other the 'accumulator'. The
	aggregation method is assumed to merge the addend
	into the accumulator. The sort then discards the addend
	row.
	<p>
	So, for the query:
	<pre><blockquote>
		select a, sum(b)
		from t
		group by a
	</blockquote></pre>
	The input row to the sorter would have one column for
	a and another column for sum(b).  It is up to the caller
	to get the format of the row correct, and to initialize
	the	aggregate values correctly (null for most aggregates,
	0 for count).
	<p>
	Nulls are always considered to be ordered in a sort, that is,
	null compares equal to null, and less than anything else.

	@param implParameters  Properties which help in choosing
	implementation-specific sort options.  If null, a
	"generally useful" sort will be used.
	
	@param template  A row which is prototypical for the sort.
	All rows inserted into the sort controller must have 
	exactly the same number of columns as the template row.
	Every column in an inserted row must have the same type
	as the corresponding column in the template.

    @param columnOrdering  An array which specifies which columns
	participate in ordering - see interface ColumnOrdering for
	details.  The column referenced in the 0th columnOrdering
	object is compared first, then the 1st, etc.  To sort on a single
    column specify an array with a single entry.

    @param sortObserver  An object that is used to observe
	the sort.  It is used to provide a callback into the sorter.
	If the sortObserver is null, then the sort proceeds as normal.
	If the sortObserver is non null, then it is called as 
	rows are loaded into the sorter.  It can be used to implement
	a distinct sort, aggregates, etc.

    @param alreadyInOrder  Indicates that the rows inserted into
	the sort controller will already be in order.  This is used
	to perform aggregation only.

    @param estimatedRows  The number of rows that the caller 
	estimates will be inserted into the sort.  -1 indicates that
	the caller has no idea.  Used by the sort to make good choices
	about in-memory vs. external sorting, and to size merge runs.

    @param estimatedRowSize  The estimated average row size of the
    rows being sorted.  This is the client portion of the rowsize, it should
    not attempt to calculate Store's overhead.  -1 indicates that the caller
    has no idea (and the sorter will use 100 bytes in that case.  Used by the 
    sort to make good choices about in-memory vs. external sorting, and to size
    merge runs.  The client is not expected to estimate the per column/
    per row overhead of raw store, just to make a guess about the storage
    associated with each row (ie. reasonable estimates for some implementations
    would be 4 for int, 8 for long, 102 for char(100),
    202 for varchar(200), a number out of hat for user types, ...).

    @return The sort identifier which can be used subsequently to
	open sort controllers and scans.
	
 	@see SortObserver
	@see ColumnOrdering
	@see ScanController
    @see SortController

 	@exception StandardException From a lower-level exception.
	**/
	long createSort(
    Properties              implParameters,
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    SortObserver            sortObserver,
    boolean                 alreadyInOrder,
    long                    estimatedRows,
    int                     estimatedRowSize)
        throws StandardException;
	/**
	Drop a sort. 
    <p>
    Drop a sort created by a call to createSort() within the current 
    transaction (sorts are automatically "dropped" at the end of a 
    transaction.  This call should only be made after all openSortScan()'s
    and openSort()'s have been closed.

    @param sortid The identifier of the sort to drop, as returned from 
                  createSort.
    <p>
 	@exception StandardException From a lower-level exception.
	**/
    void dropSort(long sortid) throws StandardException;

	/**
	Open a sort controller for a sort previously created in this
	transaction.  Sort controllers are used to insert rows into
	the sort.
	<p>
	There may (in the future) be multiple sort inserters
	for a given sort, the idea being that the various threads of
	a parallel query plan can all insert into the sort.  For now,
	however, only a single sort controller per sort is supported.

    @param id The identifier of the sort to open, as returned from
	createSort.

    @return A sort controller to use for inserting.

 	@exception StandardException From a lower-level exception.
	**/
	
	SortController openSort(long id)
		throws StandardException;

    /**
     * Return an open SortCostController.
     * <p>
     * Return an open SortCostController which can be used to ask about 
     * the estimated costs of SortController() operations.
     * <p>
     * @param implParameters  Properties which help in choosing 
     *                        implementation-specific sort options.  If null, a
	 *                        "generally useful" sort will be used.
     *
	 * @return The open StoreCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see StoreCostController
     **/
    SortCostController openSortCostController(
    Properties  implParameters)
		throws StandardException;

	/**
	Open a scan for retrieving rows from a sort.  Returns a RowSource for
	retrieving rows from the sort.

    @param id  The identifier of the sort to scan, as returned
	from createSort.

    @return The RowSource

 	@exception StandardException From a lower-level exception.
	**/
	RowLocationRetRowSource openSortRowSource(long id)
		 throws StandardException;

	/**
	Open a scan for retrieving rows from a sort.  Returns a
	scan controller for retrieving rows from the sort (NOTE:
	the only legal methods to use on the returned sort controller
	are next() and fetch() - probably there should be scan
	controllers and updatable scan controllers).
	<p>
	In the future, multiple sort scans on the same sort will
	be supported (for parallel execution across a uniqueness
	sort in which the order of the resulting rows is not
	important).  Currently, only a single sort scan is allowed
	per sort.
	<p>
	In the future, it will be possible to open a sort scan
	and start retrieving rows before the last row is inserted.
	The sort controller would block till rows were available
	to return.  Currently, an attempt to retrieve a row before
	the sort controller is closed will cause an exception.

    @param id   The identifier of the sort to scan, as returned from createSort.
	@param hold If true, this scan will be maintained open over commits.

    @return The sort controller.

 	@exception StandardException From a lower-level exception.
	**/

	ScanController openSortScan(
    long    id,
    boolean hold)
		throws StandardException;


    /**************************************************************************
     * Interfaces previously defined in TcTransactionIface:
     **************************************************************************
     */

	/**
	Return true if any transaction is blocked (even if not by this one).

	*/
	public boolean anyoneBlocked();

	/**
	Abort all changes made by this transaction since the last commit, abort
	or the point the transaction was started, whichever is the most recent.
	All savepoints within this transaction are released, and all resources
	are released (held or non-held).

	@exception StandardException Only exceptions with severities greater than
	ExceptionSeverity.TRANSACTION_SEVERITY will be thrown.
	**/
	public void abort()
		throws StandardException;

	/**
	Commit this transaction.  All savepoints within this transaction are 
    released.  All non-held conglomerates and scans are closed.

	@exception StandardException Only exceptions with severities greater than
	ExceptionSeverity.TRANSACTION_SEVERITY will be thrown.
	If an exception is thrown, the transaction will not (necessarily) have 
    been aborted.  The standard error handling mechanism is expected to do the 
    appropriate cleanup.  In other words, if commit() encounters an error, the 
    exception is propagated up to the the standard exception handler, which 
    initiates cleanupOnError() processing, which will eventually abort the 
    transaction.
	**/
	public void commit()
		throws StandardException;

	/**
	"Commit" this transaction without sync'ing the log.  Everything else is
	identical to commit(), use this at your own risk.

	<BR>bits in the commitflag can turn on to fine tuned the "commit":
	KEEP_LOCKS                          - no locks will be released by the 
                                          commit and no post commit processing 
                                          will be initiated.  If, for some 
                                          reasons, the locks cannot be kept 
                                          even if this flag is set, then the 
                                          commit will sync the log, i.e., it 
                                          will revert to the normal commit.

    READONLY_TRANSACTION_INITIALIZATION - Special case used for processing
                                          while creating the transaction.  
                                          Should only be used by the system
                                          while creating the transaction to
                                          commit readonly work that may have
                                          been done using the transaction
                                          while getting it setup to be used
                                          by the user.  In the future we should
                                          instead use a separate tranaction to
                                          do this initialization.  Will fail
                                          if called on a transaction which
                                          has done any updates.
	@see TransactionController#commit

	@exception StandardException Only exceptions with severities greater than
	ExceptionSeverity.TRANSACTION_SEVERITY will be thrown.
	If an exception is thrown, the transaction will not (necessarily) have 
    been aborted.  The standard error handling mechanism is expected to do the 
    appropriate cleanup.  In other words, if commit() encounters an error, the 
    exception is propagated up to the the standard exception handler, which 
    initiates cleanupOnError() processing, which will eventually abort the 
    transaction.
	**/
	public DatabaseInstant commitNoSync(int commitflag)
		throws StandardException;

	public final int RELEASE_LOCKS                          = 0x1;
	public final int KEEP_LOCKS                             = 0x2;
    public final int READONLY_TRANSACTION_INITIALIZATION    = 0x4;

	/**
	Abort the current transaction and pop the context.
	**/
	public void destroy();

    /**
     * Get the context manager that the transaction was created with.
     * <p>
     *
	 * @return The context manager that the transaction was created with.
     *
     **/
    public ContextManager getContextManager();

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
     *
	 * @return The a string which identifies the transaction.  
     **/
    public String getTransactionIdString();

	/**
     * Get string id of the transaction that would be when the Transaction
	 * is IN active state. This method increments the Tx id of  current Tx
	 * object if it is in idle state. 
	 * Note: Use this method only  getTransactionIdString() is not suitable.
	 * @return The string which identifies the transaction.  
     **/
    public String getActiveStateTxIdString();
    

    /**
     * Reveals whether the transaction has ever read or written data.
     *
	 * @return true If the transaction has never read or written data.
     **/
	boolean isIdle();

    /**
     * Reveals whether the transaction is a global or local transaction.
     *
	 * @return true If the transaction was either started by 
     *         AccessFactory.startXATransaction() or was morphed to a global
     *         transaction by calling createXATransactionFromLocalTransaction().
     * 
     * @see AccessFactory#startXATransaction
     * @see TransactionController#createXATransactionFromLocalTransaction
     *
     **/
	boolean isGlobal();

    /**
     * Reveals whether the transaction is read only.
     *
	 * @return true If the transaction is read only to this point.
     *
     **/
	boolean isPristine();

	/**
	Release the save point of the given name. Releasing a savepoint removes all
	knowledge from this transaction of the named savepoint and any savepoints
	set since the named savepoint was set.

    @param name     The user provided name of the savepoint, set by the user
                    in the setSavePoint() call.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
    @return returns savepoint position in the stack.

	@exception StandardException  Standard cloudscape exception policy.  A 
                                  statement level exception is thrown if
                                  no savepoint exists with the given name.
	**/
	public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
	Rollback all changes made since the named savepoint was set. The named
	savepoint is not released, it remains valid within this transaction, and
	thus can be named it future rollbackToSavePoint() calls. Any savepoints
	set since this named savepoint are released (and their changes rolled back).
    <p>
    if "close_controllers" is true then all conglomerates and scans are closed
    (held or non-held).  
    <p>
    If "close_controllers" is false then no cleanup is done by the 
    TransactionController.  It is then the responsibility of the caller to
    close all resources that may have been affected by the statements 
    backed out by the call.  This option is meant to be used by the Language
    implementation of statement level backout, where the system "knows" what
    could be affected by the scope of the statements executed within the 
    statement.
    <p>

    @param name               The identifier of the SavePoint to roll back to.
    @param close_controllers  boolean indicating whether or not the controller 
                              should close open controllers.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	  Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	  A String value for kindOfSavepoint would mean it is SQL savepoint
	  A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
    @return returns savepoint position in the stack.

	@exception StandardException  Standard cloudscape exception policy.  A 
                                  statement level exception is thrown if
                                  no savepoint exists with the given name.
	**/
	public int rollbackToSavePoint(
    String  name,
    boolean close_controllers, Object kindOfSavepoint)
        throws StandardException;


	/**
	Set a save point in the current transaction. A save point defines a point in
	time in the transaction that changes can be rolled back to. Savepoints
	can be nested and they behave like a stack. Setting save points "one" and
	"two" and the rolling back "one" will rollback all the changes made since
	"one" (including those made since "two") and release savepoint "two".

    @param name     The user provided name of the savepoint.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	  Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	  A String value for kindOfSavepoint would mean it is SQL savepoint
	  A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
	@return returns savepoint position in the stack.

	@exception StandardException  Standard cloudscape exception policy.  A 
                                  statement level exception is thrown if
                                  no savepoint exists with the given name.
	**/
	public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException;

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
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/
	/* XATransactionController */ Object createXATransactionFromLocalTransaction(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id)
		throws StandardException;

}
