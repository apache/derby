/*

   Derby - Class org.apache.derby.iapi.store.raw.Transaction

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.access.FileResource;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.services.context.ContextManager;

import java.util.Properties;

import org.apache.derby.iapi.services.property.PersistentSet;

import org.apache.derby.iapi.error.ExceptionSeverity;
/**
*/

public interface Transaction {

	/**
		Return the context manager this transaction is associated with.
	*/
	public ContextManager getContextManager();

    /**
     * Get the compatibility space of the transaction.
     * <p>
     * Returns an object that can be used with the lock manager to provide
     * the compatibility space of a transaction.  2 transactions with the
     * same compatibility space will not conflict in locks.  The usual case
     * is that each transaction has it's own unique compatibility space.
     * <p>
     *
	 * @return The compatibility space of the transaction.
     **/
    Object getCompatibilitySpace();


	/**
		Called after the transaction has been attached to an Access Manger
		TransactionController. Thus may not be called for all transactions.
		Purpose is to allow a transaction access to database (service) properties.

		Will not be called for transactions early in the boot process, ie. before
		the property conglomerate is set up.
		@exception StandardException  Standard cloudscape exception policy
	*/
	public void setup(PersistentSet set)
		throws StandardException;

	/**
		Return my transaction identifier. Transaction identifiers may be 
        re-used for transactions that do not modify the raw store.
		May return null if this transaction has no globalId.
	*/
	public GlobalTransactionId getGlobalId();

	/**
		Get the current default locking policy for all operations within this
		transaction. The transaction is initially started with a default
		locking policy equivalent to
		<PRE>
			 newLockingPolicy(
              LockingPolicy.MODE_RECORD, LockingPolicy.ISOLATION_SERIALIZABLE, true);
		</PRE>
        This default can be changed by subsequent calls to 
        setDefaultLockingPolicy(LockingPolicy policy).


		@return The current default locking policy in this transaction.
	*/

	public LockingPolicy getDefaultLockingPolicy();


	/**
		Obtain a locking policy for use in openContainer(). The mode
		and isolation must be constants from LockingPolicy. If higherOK is true
		then the object returned may implement a stricter form of locking than
		the one requested.
		<BR>
		A null LockingPolicy reference is identical to a LockingPolicy obtained 
        by using MODE_NONE which is guaranteed to exist.

		@param mode A constant of the form LockingPolicy.MODE_*
		@param isolation A constant of the form LockingPolicy.ISOLATION_*
		@param stricterOK True if a stricter level of locking is acceptable, 
        false if an exact match is required.

		@return A object that can be used in an openContainer call, 
        null if a matching policy cannot be found.
	*/

	public LockingPolicy newLockingPolicy(int mode, int isolation, boolean stricterOk);


	/**
		Set the default locking policy for all operations within this
		transaction. The transaction is intially started with a default
		locking policy equivalent to
		<PRE>
			 newLockingPolicy(
              LockingPolicy.MODE_RECORD, LockingPolicy.ISOLATION_SERIALIZABLE, true);
		</PRE>

		@param policy The lock policy to use, if null then then a no locking 
        policy will be installed as the default.

		@return true if a new locking policy was installed as the default, false
		of a matching policy could not be found.
	*/

	public void setDefaultLockingPolicy(LockingPolicy policy);


	/**
		Commit this transaction. All savepoints within this transaction are 
        released.

		@return the commit instant of this transaction, or null if it
		didn't make any changes 
		
		@exception StandardException
        A transaction level exception is thrown
        if the transaction was aborted due to some error. Any exceptions that 
        occur of lower severity than Transaction severity are caught, the 
        transaction is then aborted and then an exception of Transaction
		severity is thrown nesting the original exception.

		@exception StandardException Any exception more severe than a
        Transaction exception is not caught and the transaction is not aborted.
        The transaction will be aborted by the standard context mechanism.

	*/

	public LogInstant commit() throws StandardException;

	/**
	    "Commit" this transaction without sync'ing the log.
		Everything else is identical to commit(), use this at your own risk.
		
		<BR>bits in the commitflag can turn on to fine tuned the "commit":
		KEEP_LOCKS - no locks will be released by the commit and no post commit
		processing will be initiated.  If, for some reasons, the locks cannot be
		kept even if this flag is set, then the commit will sync the log, i.e.,
		it will revert to the normal commit.

		@exception StandardException
        A transaction level exception is thrown
        if the transaction was aborted due to some error. Any exceptions that 
        occur of lower severity than Transaction severity are caught, the 
        transaction is then aborted and then an exception of Transaction
		severity is thrown nesting the original exception.

		@exception StandardException Any exception more severe than a
        Transaction exception is not caught and the transaction is not aborted.
        The transaction will be aborted by the standard context mechanism.
	*/

	public LogInstant commitNoSync(int commitflag) throws StandardException;
	public final int RELEASE_LOCKS = TransactionController.RELEASE_LOCKS;
	public final int KEEP_LOCKS = TransactionController.KEEP_LOCKS;


	/**
		Abort all changes made by this transaction since the last commit, abort
		or the point the transaction was started, whichever is the most recent.
		All savepoints within this transaction are released.

		@exception StandardException Only exceptions with severities greater 
        than ExceptionSeverity.TRANSACTION_SEVERITY will be thrown.
		
	*/
	public void abort() throws StandardException;

	/**
		Close this transaction, the transaction must be idle. This close will
		pop the transaction context off the stack that was pushed when the 
        transaction was started.

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Cloudscape error policy
		@exception StandardException A transaction level exception is 
        thrown if the transaction is not idle.

		
	*/
	public void close() throws StandardException;

	/**
        If this transaction is not idle, abort it.  After this call close().

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Cloudscape error policy
		@exception StandardException A transaction level exception is 
        thrown if the transaction is not idle.

		
	*/
	public void destroy() throws StandardException;


	/**
		Set a save point in the current transaction. A save point defines a 
        point in time in the transaction that changes can be rolled back to. 
        Savepoints can be nested and they behave like a stack. Setting save 
        points "one" and "two" and the rolling back "one" will rollback all 
        the changes made since "one" (including those made since "two") and 
        release savepoint "two".
    @param name     The user provided name of the savepoint
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard cloudscape exception policy
		@exception StandardException
        A statement level exception is thrown if a savepoint already 
        exists in the current transaction with the same name.
		
	*/

	public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
		Release the save point of the given name. Relasing a savepoint removes 
        all knowledge from this transaction of the named savepoint and any 
        savepoints set since the named savepoint was set.
    @param name     The user provided name of the savepoint, set by the user
                    in the setSavePoint() call.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard cloudscape exception policy
		@exception StandardException
        A statement level exception is thrown if a savepoint already
        exists in the current transaction with the same name.

	*/

	public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
		Rollback all changes made since the named savepoint was set. The named
		savepoint is not released, it remains valid within this transaction, and
		thus can be named it future rollbackToSavePoint() calls. Any savepoints
		set since this named savepoint are released (and their changes rolled
        back).
    @param name     The user provided name of the savepoint, set by the user
                    in the setSavePoint() call.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard cloudscape exception policy
		@exception StandardException
        A statement level exception is thrown if no savepoint exists with 
        the given name.

	*/
	public int rollbackToSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**

		Open a container, with the transaction's default locking policy.

        <p>
        Note that if NOWAIT has been specified lock will be 
        requested with no wait time, and if lock is not granted a 
        SQLState.LOCK_TIMEOUT exception will be thrown.
		<P>
		The release() method of ContainerHandle will be called when this 
        transaction is aborted or commited, it may be called explicitly to
		release the ContainerHandle before the end of the transaction.


		@return a valid ContainerHandle or null if the container does not exist.

		@exception StandardException  Standard cloudscape exception policy

	*/
	public ContainerHandle openContainer(ContainerKey containerId,  int mode)
		throws StandardException;

	/**

		Open a container, with the defined locking policy, otherwise
		as openContainer(int containerId,  boolean forUpdate).

		<P>
		Calls locking.lockContainer(this, returnValue, forUpdate) to lock the
        container.  Note that if NOWAIT has been specified lock will be 
        requested with no wait time, and if lock is not granted a 
        SQLState.LOCK_TIMEOUT exception will be thrown.

		@param policy The lock policy to use, if null then then a no locking 
        policy will be used.

		@return a valid ContainerHandle or null if the container does not exist.

		@exception StandardException  Standard cloudscape exception policy

	*/

	public ContainerHandle openContainer(ContainerKey containerId,
										 LockingPolicy locking, int mode) 
		throws StandardException;


	/**
		Add a new container to the segment. The new container initially has
		one page, page Container.FIRST_PAGE_NUMBER.

		<BR>
		If pageSize is equal to ContainerHandle.DEFAULT_PAGESIZE or invalid 
        then a default page size will be picked.
		<BR>
		SpareSpace indicates that percent (0% - 100%) of page space that will 
        be attempted to be reserved for updates. E.g. with a value of 20 a page
        that would normally hold 40 rows will be limited to 32 rows,
		actual calculation for the threshold where no more inserts are all 
        accepted is up to the implementation.  Whatever the value of 
        spaceSpace an empty page will always accept at least one insert.
		If spare space is equal to ContainerHandle.DEFAULT_PAGESIZE or invalid 
        then a default value will be used.

	    <P><B>Synchronisation</B>
		<P>
		The new container is exclusivly locked by this transaction until
		it commits.

		@param segmentId    segment to create the container in.
		@param containerId  If not equal to 0 then this container id will be 
                            used to create the container, else if set to 0 then
                            the raw store will assign a number.
		@param mode mode description in @see ContainerHandle.  This mode is
		only effective for the duration of the addContainer call and not stored
		persistently for the lifetime of the container.
		@param tableProperties Implementation-specific properties of the
		conglomerate.

		@return a container identifer that can be used in openContainer()
		This id is only valid within this RawStoreFactory.  Returns a negative 
        number if a container could not be allocated.

		@exception StandardException Standard Cloudscape error policy

	*/
	public long addContainer(
    long        segmentId, 
    long        containerId,
    int         mode, 
    Properties  tableProperties, 
    int         temporaryFlag) 
		throws StandardException;

	/**
		Drop a container.

	    <P><B>Synchronisation</B>
		<P>
		This call will mark the container as dropped and then obtain an CX lock
		on the container. Once a container has been marked as dropped it cannot
		be retrieved by any openContainer() call.
		<P>
		Once the exclusive lock has been obtained the container is removed
		and all its pages deallocated. The container will be fully removed
		at the commit time of the transaction.

		@exception StandardException Standard Cloudscape error policy

	*/
	public void dropContainer(ContainerKey containerId)
		throws StandardException;

	/**
		Add a new stream container to the segment and load the stream container.
		
		This stream container doesn't not have locks, and do not log.
		It does not have the concept of a page.
		It is used by the external sort only.

	    <P><B>Synchronisation</B>
		<P>
		This call will mark the container as dropped and then obtain an CX lock
		on the container. Once a container has been marked as dropped it cannot
		be retrieved by any openContainer() call.
		<P>
		Once the exclusive lock has been obtained the container is removed
		and all its pages deallocated. The container will be fully removed
		at the commit time of the transaction.

		@exception StandardException Standard Cloudscape error policy

	*/
	public long addAndLoadStreamContainer(
			long segmentId, Properties tableProperties, RowSource rowSource)
		throws StandardException;


	/**
		Open a stream container.

		@return a valid StreamContainerHandle or null if the container does not exist.

		@exception StandardException  Standard cloudscape exception policy

	*/
	public StreamContainerHandle openStreamContainer(
    long    segmentId, 
    long    containerId,
    boolean hold)
		throws StandardException;

	/**
		Drop a stream container.

	    <P><B>Synchronisation</B>
		<P>
		This call will remove the container.

		@exception StandardException Standard Cloudscape error policy

	*/
	public abstract void dropStreamContainer(long segmentId, long containerId)
		throws StandardException;
		
	/**
		Log an operation and then action it in the context of this transaction.
		The Loggable Operation is logged in the transaction log file and then 
        its doMe method is called to perform the required change. If this 
        transaction aborts or a rollback is performed of the current savepoint 
        (if any) then a compensation Operation needs to be generated that will 
        compensate for the change of this Operation. 

		@param operation the operation that is to be applied

		@see Loggable

		@exception StandardException  Standard cloudscape exception policy

	*/
	public void logAndDo(Loggable operation) throws StandardException;


	/**
		Add to the list of post commit work that may be processed after this
		transaction commits.  If this transaction aborts, then the post commit
		work list will be thrown away.  No post commit work will be taken out
		on a rollback to save point.

		@param work the post commit work that is added
	*/
	public void addPostCommitWork(Serviceable work);

	/**
		Add to the list of post termination work that may be processed after this
		transaction commits or aborts.

		@param work the post termination work that is added
	*/
	public void addPostTerminationWork(Serviceable work);

    /**
     * Reveals whether the transaction has ever read or written data.
     *
	 * @return true If the transaction has never read or written data.
     **/
	public boolean isIdle();

    /**
	  Reveal whether the transaction is in a pristine state, which
	  means it hasn't done any updates since the last commit.
	  @return true if so, false otherwise
	  */
    public boolean isPristine();

	/**
		Get an object to handle non-transactional files.
	*/
	public FileResource getFileHandler();

	/**
		Get cache statistics for the specified cache
	*/
	public abstract long[] getCacheStats(String cacheName);

	/**
		Reset the cache statistics for the specified cache
	*/
	public abstract void resetCacheStats(String cacheName);

	/**
		Return true if any transaction is blocked, even if not by this one.
	 */
	public  boolean anyoneBlocked();

	/**
     * Convert a local transaction to a global transaction.
     * <p>
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Tbis controller allows one to manipulate a 
     * global XA conforming transaction.
     * <p>
     * Must only be called a previous local transaction was created and exists
     * in the context.  Can only be called if the current transaction is in
     * the idle state.  
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.  
     *
     * @param cm        The context manager for the current context.
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 **/
	void createXATransactionFromLocalTransaction(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id)
		throws StandardException;

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 current xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_commit(
    boolean onePhase)
		throws StandardException;

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
		throws StandardException;
    public static final int XA_RDONLY = 1; 
    public static final int XA_OK     = 2; 

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
        throws StandardException;

	
    /**
	 * get string ID of the actual transaction ID that will 
	 * be used when transaction is in  active state. 
	 */
	public String getActiveStateTxIdString();



}
