/*

   Derby - Class org.apache.derby.impl.store.raw.data.OverflowInputStream

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.access.TransactionController;

import java.io.InputStream;
import java.io.IOException;

/**
	A OverflowInputStream is used by store to turn a long column
	into an InputStream.
*/
public class OverflowInputStream
extends BufferedByteHolderInputStream
implements Resetable
{
	protected BaseContainerHandle owner;
	protected long overflowPage;
	protected int overflowId;
    // remember first page and id for reset
	protected long firstOverflowPage;
	protected int firstOverflowId;
    // the row to lock for Blobs/Clobs
    protected RecordHandle recordToLock;

	public OverflowInputStream(ByteHolder bh, BaseContainerHandle owner,
		    long overflowPage, int overflowId, RecordHandle recordToLock)
        throws IOException, StandardException
	{
		super(bh);
		this.owner = owner;
		this.overflowPage = overflowPage;
		this.overflowId = overflowId;
		this.firstOverflowPage = overflowPage;
		this.firstOverflowId = overflowId;
        this.recordToLock = recordToLock;
		fillByteHolder();
	}


	public void fillByteHolder() throws IOException
	{
		if ((this.bh.available() == 0) && (this.overflowPage != -1))
        {
			this.bh.clear();

			try
            {
				// fill the byte holder with data from the page.
				BasePage columnOverflowPage = ((BasePage) this.owner.getPage(overflowPage));

				if (columnOverflowPage != null)
                {
					columnOverflowPage.restorePortionLongColumn(this);
					columnOverflowPage.unlatch();
					columnOverflowPage = null;
				}
			}
            catch (StandardException se)
            {
				throw new IOException( se.toString() );
			}
			this.bh.startReading();
		}
	}


	public long getOverflowPage() {
		return this.overflowPage;
	}

	public int getOverflowId() {
		return this.overflowId;
	}

	public void setOverflowPage(long overflowPage) {
		this.overflowPage = overflowPage;
	}

	public void setOverflowId(int overflowId) {
		this.overflowId = overflowId;
	}


    /*
     Methods of Resetable interface.
    */

    /*
     Resets the stream to the beginning.
     */
    public void resetStream() throws IOException, StandardException
    {
        // check the container is open, this is needed to make sure the
        // container closed exception is thrown as a StandardException and not
        // as an IOException
        owner.checkOpen();
        // return to the original overflow page and id
		this.overflowPage = firstOverflowPage;
		this.overflowId = firstOverflowId;
        // completely clear the byte holder
        this.bh.clear();
        this.bh.startReading();
        // fill the byte holder
		fillByteHolder();
    }

    /*
      Initialize.  Reopen the container. This will have the effect of
      getting an intent shared lock on the table, which will stay around until
      the end of the transaction (or until the enclosing blob/clob object is
      closed). Also get a read lock on the appropriate row.
    */
    public void initStream() throws StandardException
    {
        /*
        We might want to use the mode and isolation level of the container.
        This would have the advantage that, if the isolation level
        is READ_COMMITTED, resources would be freed if blobs/clob finalizers are
        called (e.g. they are garbage collected) before the end of transaction.
        If the mode was MODE_CONTAINER, openContainer would get an S lock on the
        table instead of an IS lock, and lockRecordForRead would have no effect.

        To do this, need to consider:
        Sometimes the container's locking policy may NOT reflect the correct
        locking policy. For example, if the container is a table (not an index)
        and Access handles the locking of the table via an index, the container's
        locking policy would be set to do no locking.
        Moreover, if the container is an index, the locking policy would
        always be set to do no locking.
        */

        LockingPolicy lp = 
            owner.getTransaction().newLockingPolicy(
                LockingPolicy.MODE_RECORD, 
                TransactionController.ISOLATION_REPEATABLE_READ, true);

        // reopen the container
        owner = (BaseContainerHandle) owner.getTransaction().openContainer(
            owner.getId(), lp, owner.getMode());

        // get a read lock on the appropriate row
        // this will wait until either the lock is granted or an exception is
        // thrown
        owner.getLockingPolicy().lockRecordForRead(
            owner.getTransaction(), owner, recordToLock, true, false);
    }


    /*
      Close the container associated with this stream. (In the future if we use
      a read committed isolation mode, this will also free the associated IS
      table lock and the associated S row lock.)
    */
    public void closeStream()
    {
        owner.close();
    }

}
