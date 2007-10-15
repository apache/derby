/*

   Derby - Class org.apache.derby.impl.store.raw.data.OverflowInputStream

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.Resetable;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.access.TransactionController;

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
    
    // Make sure record is only locked once.
    private boolean initialized = false;

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
				BasePage columnOverflowPage = 
                    ((BasePage) this.owner.getPage(overflowPage));

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

    /**
     * Initialize.  Reopen the container. This will have the effect of
     * getting an intent shared lock on the table, which will stay around until
     * the enclosing blob/clob object is closed, or until the end of the 
     * transaction. Also get a read lock on the appropriate row.
     * 
     * @throws org.apache.derby.iapi.error.StandardException
     */
    public void initStream() throws StandardException
    {
        if (initialized) return;
        
        // it is possible that the transaction in which the stream was
        // created is committed and no longer valid
        // dont want to get NPE but instead throw error that
        // container was not opened
        if (owner.getTransaction() == null)
            throw StandardException.newException(SQLState.DATA_CONTAINER_CLOSED);
                
        /*
        We use isolation level READ_COMMITTED and reopen the container to 
        get a new container handle to use for locking.  This way, the lock will
        be freed when we the container handle is closed. This will happen in
        closeStream() or when the transaction commits. 
        Hence, locks will be released before the end of transaction if 
        blobs/clobs are explicitly released.
        */
        LockingPolicy lp = 
            owner.getTransaction().newLockingPolicy(
                LockingPolicy.MODE_RECORD, 
                TransactionController.ISOLATION_READ_COMMITTED, true);

        // reopen the container
        owner = (BaseContainerHandle) owner.getTransaction().openContainer(
            owner.getId(), lp, owner.getMode());

        // get a read lock on the appropriate row
        // this will wait until either the lock is granted or an exception is
        // thrown
        owner.getLockingPolicy().lockRecordForRead(
            owner.getTransaction(), owner, recordToLock, true, false);
        
        initialized = true;
    }


    /*
      Close the container associated with this stream. (This will also free the 
      associated IS table lock and the associated S row lock.)
    */
    public void closeStream()
    {
        owner.close();
        initialized = false;
    }

}
