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

import org.apache.derby.iapi.services.io.CloneableStream;

import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.types.Resetable;

import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.access.TransactionController;

import java.io.InputStream;
import java.io.IOException;

/**
A OverflowInputStream is used by store to turn a long column into an 
InputStream.
<p>
Any time store fetches a long column, the value is returned as a stream.
A long column is any column that at some point was longer than a page, so
a long column in one table may not be long in another depending on page size.
<p)
When the column is fetched a new OverflowInputStream is created and then
the datatype's stream is set using:
 ((StreamStorable)sColumn).setStream(OverflowInputStream);

**/

public class OverflowInputStream
extends BufferedByteHolderInputStream
implements Resetable, CloneableStream
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    protected BaseContainerHandle   owner;

    // tracks the next overflow page and id on that page to read
    protected long                  overflowPage;
    protected int                   overflowId;

    // remember first page and id for reset
    protected long                  firstOverflowPage;
    protected int                   firstOverflowId;

    // the row to lock for Blobs/Clobs
    protected RecordHandle          recordToLock;

    // Make sure record is only locked once.
    private boolean initialized = false;


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * Constructor for OverflowInputStream
     * <p>
     * It is up to the caller to allocate the ByteHolder for this stream,
     * and pass it in.
     *
     * @param bh            ByteHolder to hold buffers of bytes as the stream is
     *                      walked.  Expected usage is that a page worth of 
     *                      data is held in memory at a time, filled by a 
     *                      calls to restorePortionLongColumn(). 
     *
     * @param owner         BaseContainerHandle used to read pages from the 
     *                      container.  Note this handle is closed automatically
     *                      on commit.
     *
     * @param overflowPage  The first overflow page of this long column.  
     *
     * @param overflowId    The record id of 1st segment of the long column on
     *                      overflowPage.
     *
     * @param recordToLock  RecordHandle of the owning record of the long long
     *                      column, this is the row level lock to get.
     **/
    public OverflowInputStream(
    ByteHolder          bh, 
    BaseContainerHandle owner,
    long                overflowPage, 
    int                 overflowId, 
    RecordHandle        recordToLock) {
        super(bh);
        this.owner              = owner;
        this.overflowPage       = overflowPage;
        this.overflowId         = overflowId;
        this.firstOverflowPage  = overflowPage;
        this.firstOverflowId    = overflowId;
        this.recordToLock       = recordToLock;
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * If bytes remain in stream, insure the current buffer is not empty.
     * <p>
     * If there are bytes in current buffer than no more work necessary,
     * else if there are no bytes available in current buffer and there are 
     * still more overflow segments then get the next buffer's worth of
     * data.
     *
     * @exception  IOException
     **/
    public void fillByteHolder() 
        throws IOException
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
                // Simplify this code when we can use the Java 1.5 constructor
                // taking the cause as an argument.
                IOException ioe = new IOException(se.toString());
                ioe.initCause(se);
                throw ioe;
            }

            this.bh.startReading();
        }
    }

    /**
     * Set the next overflow page of the long column.
     * <p>
     * Used by StorePage.restorePortionLongColumn() as part of the call back
     * process to save the state of the scan of the pieces of the long column.
     * StorePage.restorePortionLongColumn() is called by fillByteHolder() to
     * get the next page worth into a buffer, and in turn after those bytes
     * are read the state of this stream is updated with then next overflow
     * page.
     *
     * @param overflowPage  Page number containing the next segment of the
     *                      long column.  -1 if there are no more segments.
     *
     **/
    public void setOverflowPage(long overflowPage) 
    {
        this.overflowPage = overflowPage;
    }

    /**
     * Set the next overflow page of the long column.
     * <p>
     * Used by StorePage.restorePortionLongColumn() as part of the call back
     * process to save the state of the scan of the pieces of the long column.
     * StorePage.restorePortionLongColumn() is called by fillByteHolder() to
     * get the next page worth into a buffer, and in turn after those bytes
     * are read the state of this stream is updated with then next overflow
     * page.
     *
     * @param overflowId    Page number containing the next segment of the
     *                      long column.  -1 if there are no more segments.
     *
     **/
    public void setOverflowId(int overflowId) 
    {
        this.overflowId = overflowId;
    }

    public long getOverflowPage() 
    {
        return this.overflowPage;
    }

    public int getOverflowId() 
    {
        return this.overflowId;
    }

    /**************************************************************************
     * Public Methods of Resetable Interface
     **************************************************************************
     */

    /**
     * Initialize a Resetable stream.
     * <p>
     * InitStream() must be called first before using any other of the 
     * Resetable interfaces.
     * <p>
     * Reopens the container.  This gets a separate intent shared locked on
     * the table and a read lock on the appropriate row.  These locks remain
     * until the enclosing blob/clob object is closed, or until the end of
     * the transaction in which initStream() was first called.  This locking
     * behavior protects the row while the stream is being accessed.  Otherwise
     * for instance in the case of read committed the original row lock on 
     * the row would be released when the scan went to the next row, and there
     * would be nothing to stop another transaction from deleting the row while
     * the client read through the stream.
     *
     * @exception  StandardException  Standard exception policy.
     **/

    public void initStream() throws StandardException
    {
        // only one initStream() required.
        if (initialized) 
            return;

        // it is possible that the transaction in which the stream was
        // created is committed and no longer valid dont want to get NPE but 
        // instead throw error that container was not opened
        if (owner.getTransaction() == null)
        {
            throw StandardException.newException(
                    SQLState.DATA_CONTAINER_CLOSED);
        }

        // Use isolation level READ_COMMITTED and reopen the container to 
        // get a new container handle to use for locking.  This way, the lock 
        // will be freed when we the container handle is closed. This will 
        // happen in closeStream() or when the transaction commits. 
        // Hence, locks will be released before the end of transaction if 
        // blobs/clobs are explicitly released.
        LockingPolicy lp = 
            owner.getTransaction().newLockingPolicy(
                LockingPolicy.MODE_RECORD, 
                TransactionController.ISOLATION_READ_COMMITTED, 
                true);

        // reopen the container
        owner = (BaseContainerHandle) owner.getTransaction().openContainer(
                    owner.getId(), lp, owner.getMode());

        // get a read lock on the appropriate row this will wait until either 
        // the lock is granted or an exception is thrown
        owner.getLockingPolicy().lockRecordForRead(
            owner.getTransaction(), owner, recordToLock, true, false);

        initialized = true;
    }

    /**
     * Reset the stream back to beginning of the long column.
     * <p>
     * Also fills in the first buffer from the stream.
     * <p>
     * Throws exception if the underlying open container has been closed,
     * for example automatically by a commit().
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void resetStream() throws IOException, StandardException
    {
        // check the container is open, this is needed to make sure the
        // container closed exception is thrown as a StandardException and not
        // as an IOException
        owner.checkOpen();

        // return to the original overflow page and id
        this.overflowPage   = firstOverflowPage;
        this.overflowId     = firstOverflowId;

        // completely clear the byte holder
        this.bh.clear();
        this.bh.startReading();
    }

    /**
     * Close the Resetable stream.
     * <p>
     * Close the container associated with this stream.  (This will also free 
     * the associated IS table lock and the associated S row lock.)
     **/
    public void closeStream()
    {
        owner.close();
        initialized = false;
    }

    /**************************************************************************
     * Public Methods of CloneableStream Interface
     **************************************************************************/

    /**
     * Clone this object.
     * <p>
     * Creates a deep copy of this object. The returned stream has its own
     * working buffers and can be initialized, reset and read independently
     * from this stream.
     * <p>
     * The cloned stream is set back to the beginning of stream, no matter
     * where the current stream happens to be positioned.
     *
     * @return Copy of this stream which can be used independently.
     */
    public InputStream cloneStream() {
        OverflowInputStream ret_stream = 
            new OverflowInputStream(
                bh.cloneEmpty(),
                owner, 
                firstOverflowPage, 
                firstOverflowId, 
                recordToLock);

        return(ret_stream);
    }
}
