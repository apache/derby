/*

   Derby - Class org.apache.derby.impl.store.raw.data.StreamFileContainerHandle

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.locks.Lockable;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.StreamContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.xact.RawTransaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.impl.store.raw.data.DropOnCommit;

import org.apache.derby.catalog.UUID;

import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

/**
	A handle to an open stream container, implememts StreamContainerHandle.
	<P>
	This class is an Observer to observe RawTransactions

	<BR> MT - Mutable - Immutable identity - Thread Aware
*/

final class StreamFileContainerHandle 
    implements  StreamContainerHandle, Observer 
{

	/*
	** Fields
	*/

	/**
		Raw Store identifier
		<BR> MT - Immutable
	*/
	private final UUID rawStoreId;

	/**
		Container identifier
		<BR> MT - Immutable
	*/
	protected final ContainerKey identity;


	/**
		Is this StreamContainerHandle active.

		<BR> MT - Mutable : scoped
	*/
	protected boolean active;	

	/**
		The actual container we are accessing. Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/
	protected StreamFileContainer container;

	/**
		our transaction. Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/
	protected RawTransaction xact;

	/**
		Whether this container should be held open across commit.  
        Only valid when active is true.

		<BR> MT - Mutable : scoped
	*/
	private boolean     hold;

	/*
	** Constructor
	*/
	public StreamFileContainerHandle(
    UUID            rawStoreId, 
    RawTransaction  xact, 
    ContainerKey    identity,
    boolean         hold) 
    {
		this.identity   = identity;
		this.xact       = xact;
		this.rawStoreId = rawStoreId;
		this.hold       = hold;
	}

	public StreamFileContainerHandle(
    UUID                rawStoreId, 
    RawTransaction      xact, 
    StreamFileContainer container,
    boolean             hold) 
    {

		this.identity   = container.getIdentity();
		this.xact       = xact;
		this.rawStoreId = rawStoreId;
		this.hold       = hold;

		this.container  = container;

		// we are inactive until useContainer is called.
	}

	/*
	** Methods from StreamContainerHandle
	*/

    /**
     * Request the system properties associated with a container. 
	 * @see StreamContainerHandle#getContainerProperties
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void getContainerProperties(Properties prop)
		throws StandardException {

        container.getContainerProperties(prop);
        return;
    }

    /**
     * fetch a row from the container. 
    *
	 * @exception  StandardException  Standard exception policy.
     **/
	public boolean fetchNext(DataValueDescriptor[] row) 
        throws StandardException {

		return container.fetchNext(row);
	}

	/**
		@see StreamContainerHandle#close

	    @exception  StandardException  Standard exception policy.
	*/
	public void close() 
    {

        if (xact == null) {

            // Probably be closed explicitly by a client, after closing 
            // automatically after an abort.
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(!active);

            return;
        }

		active = false;

		// let go of the container
        container.close();
		container = null;

		// and remove ourseleves from this transaction
		xact.deleteObserver(this);

		xact = null;
	}

	/**
		remove the stream container

		@exception StandardException Standard Cloudscape error policy		
		@see StreamContainerHandle#removeContainer
	 */
	public void removeContainer() throws StandardException {
		container.removeContainer();
	}

	/**
		get the container key for the stream container
	 */
	public ContainerKey getId() {
		return identity;
	}

	/*
	**	Methods of Observer
	*/

	/**
		Called when the transaction is about to complete.

		@see Observer#update
	*/
	public void update(Observable obj, Object arg) 
    {
		if (SanityManager.DEBUG) {
			if (arg == null)
				SanityManager.THROWASSERT("still on observr list " + this);
		}

		// already been removed from the list
		if (xact == null) {

			return;
		}

		if (SanityManager.DEBUG) {
			// just check reference equality

			if (obj != xact)
				SanityManager.THROWASSERT("Observable passed to update is incorrect expected "
					+ xact + " got " + obj);
		}

		// close on a commit, abort or drop of this container.
		if (arg.equals(RawTransaction.COMMIT) || 
            arg.equals(RawTransaction.ABORT)  || 
            arg.equals(identity)) 
        {
			// close the container		
            close();
			return;

		}
		
		if (arg.equals(RawTransaction.SAVEPOINT_ROLLBACK)) {

			// remain open
			return;
		}
	}

	/*
	** Implementation specific methods, these are public so that they can be called
	** in other packages that are specific implementations of Data, ie.
	** a directory at the level
	**
	** com.ibm.db2j.impl.Database.Storage.RawStore.Data.*
	*/

	/**
		Attach me to a container. If this method returns false then
		I cannot be used anymore, and any reference to me must be discarded.

		@exception StandardException Standard Cloudscape error policy
	*/
	public boolean useContainer() throws StandardException {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(!active);
			SanityManager.ASSERT(container != null);
		}

		// always set forUpdate to false
		if (!container.use(this)) {
			container = null;
			return false;
		}

		active = true;

		// watch transaction and close ourseleves just before it completes.
        if (!hold)
        {
            xact.addObserver(this);
            xact.addObserver(new DropOnCommit(identity, true));
        }

		return true;
	}

	/**
		Return the RawTransaction this object was opened in.
	*/
	public final RawTransaction getTransaction() {

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(xact != null);
		}

		return xact;
	}

	/*
	** Implementation specific methods for myself and my sub-classes
	*/
	public String toString() {
        if (SanityManager.DEBUG) {
            String str = new String();
            str += "StreamContainerHandle:(" + identity.toString() + ")";
            return(str);
        } else {
            return(super.toString());
        }
    }
}

