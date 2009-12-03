/*

   Derby - Class org.apache.derby.impl.store.raw.data.RAFContainer4

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
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.ContainerKey;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedByInterruptException;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * RAFContainer4 overrides a few methods in RAFContainer in an attempt to use
 * FileChannel from Java 1.4's New IO framework to issue multiple IO operations
 * to the same file concurrently instead of strictly serializing IO operations
 * using a mutex on the container object.
 * <p>
 * Note that our requests for multiple concurrent IOs may be serialized further
 * down in the IO stack - this is entirely up to the JVM and OS. However, at
 * least in Linux on Sun's 1.4.2_09 JVM we see the desired behavior:
 * The FileChannel.read/write(ByteBuffer buf, long position) calls map to
 * pread/pwrite system calls, which enable efficient IO to the same file
 * descriptor by multiple threads.
 * <p>
 * This whole class should be merged back into RAFContainer when Derby
 * officially stops supporting Java 1.3.
 * <p>
 * Significant behavior changes from RAFContainer:
 * <ol>
 * <li> Multiple concurrent IOs permitted.
 * <li> State changes to the container (create, open, close) can now happen while
 *      IO is in progress due to the lack of locking. Closing a container while
 *      IO is in progress will cause IOExceptions in the thread calling readPage
 *      or writePage. If this happens something is probably amiss anyway.
 *      The iosInProgress variable is used in an attempt to detect this should it
 *      happen while running a debug build.
 * </ol>
 *
 * @see java.nio.channels.FileChannel
 */
class RAFContainer4 extends RAFContainer {

    /**
     * This channel will be retrieved from RAFContainer's fileData
     * member when fileData is set. We wrap a couple of RAFContainer's methods
     * to accomplish this.
     */
    private FileChannel ourChannel = null;

    /**
     * For debugging - will be incremented when an IO is started, decremented
     * when it is done. Should be == 0 when container state is changed.
     */
    private int iosInProgress = 0;

    public RAFContainer4(BaseDataFileFactory factory) {
        super(factory);
    }

    /**
     * Return the {@code FileChannel} for the specified
     * {@code StorageRandomAccessFile} if it is a {@code RandomAccessFile}.
     * Otherwise, return {@code null}.
     *
     * @param file the file to get the channel for
     * @return a {@code FileChannel} if {@code file} is an instance of
     * {@code RandomAccessFile}, {@code null} otherwise
     */
    private FileChannel getChannel(StorageRandomAccessFile file) {
        if (file instanceof RandomAccessFile) {
            /** XXX - this cast isn't testing friendly.
             * A testing class that implements StorageRandomAccessFile but isn't
             * a RandomAccessFile will be "worked around" by this class. An
             * example of such a class is
             * functionTests/util/corruptio/CorruptRandomAccessFile.java.
             * An interface rework may be necessary.
             */
            return ((RandomAccessFile) file).getChannel();
        }
        return null;
    }

    /**
     * <p>
     * Return the file channel for the current value of the {@code fileData}
     * field. If {@code fileData} doesn't support file channels, return
     * {@code null}.
     * </p>
     *
     * <p>
     * Callers of this method must synchronize on the container object since
     * two shared fields ({@code fileData} and {@code ourChannel}) are
     * accessed.
     * </p>
     *
     * @return a {@code FileChannel} object, if supported, or {@code null}
     */
    private FileChannel getChannel() {
        if (ourChannel == null) {
            ourChannel = getChannel(fileData);
        }
        return ourChannel;
    }

    /*
     * Wrapping methods that retrieve the FileChannel from RAFContainer's
     * fileData after calling the real methods in RAFContainer.
     */
    synchronized boolean openContainer(ContainerKey newIdentity)
        throws StandardException
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(iosInProgress == 0,
                    "Container opened while IO operations are in progress. "
                    + "This should not happen.");
            SanityManager.ASSERT(fileData == null, "fileData isn't null");
            SanityManager.ASSERT(ourChannel == null, "ourChannel isn't null");
        }

        return super.openContainer(newIdentity);
    }

    synchronized void createContainer(ContainerKey newIdentity)
        throws StandardException
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(iosInProgress == 0,
                    "Container created while IO operations are in progress. "
                    + "This should not happen.");
            SanityManager.ASSERT(fileData == null, "fileData isn't null");
            SanityManager.ASSERT(ourChannel == null, "ourChannel isn't null");
        }
        super.createContainer(newIdentity);
    }


    synchronized void closeContainer() {
        if (SanityManager.DEBUG) {
            // Any IOs in progress to a container being dropped will be
            // ignored, so we should not complain about starting a close
            // while there are IOs in progress if it is being dropped
            // anyway.
            SanityManager.ASSERT( (iosInProgress == 0)
                    || getCommittedDropState(),
                    "Container closed while IO operations are in progress. "
                    + " This should not happen.");
        }
        if(ourChannel != null) {
            try {
                ourChannel.close();
            } catch (IOException e) {
                // nevermind.
            } finally {
                ourChannel=null;
            }
        }
        super.closeContainer();
    }

    /**
     * These are the methods that were rewritten to use FileChannel.
     **/

    /**
     *  Read a page into the supplied array.
     *
     *  <BR> MT - thread safe
     *  @exception IOException exception reading page
     *  @exception StandardException Standard Derby error policy
     */
    protected void readPage(long pageNumber, byte[] pageData)
         throws IOException, StandardException
    {
        // If this is the first alloc page, there may be another thread
        // accessing the container information in the borrowed space on the
        // same page. In that case, we synchronize the entire method call, just
        // like RAFContainer.readPage() does, in order to avoid conflicts. For
        // all other pages it is safe to skip the synchronization, since
        // concurrent threads will access different pages and therefore don't
        // interfere with each other.
        if (pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
            synchronized (this) {
                readPage0(pageNumber, pageData);
            }
        } else {
            readPage0(pageNumber, pageData);
        }
    }

    private void readPage0(long pageNumber, byte[] pageData)
         throws IOException, StandardException
    {
        FileChannel ioChannel;
        synchronized (this) {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(!getCommittedDropState());
            }
            ioChannel = getChannel();
        }

        if(ioChannel != null) {

            long pageOffset = pageNumber * pageSize;

            ByteBuffer pageBuf = ByteBuffer.wrap(pageData);

            // I hope the try/finally is optimized away by the
            // compiler/jvm when SanityManager.DEBUG == false?
            try {
                if (SanityManager.DEBUG) {
                    synchronized(this) {
                        iosInProgress++;
                    }
                }

                readFull(pageBuf, ioChannel, pageOffset);
            }
            finally {
                if (SanityManager.DEBUG) {
                    synchronized(this) {
                        iosInProgress--;
                    }
                }

            }

            if (dataFactory.databaseEncrypted() &&
                pageNumber != FIRST_ALLOC_PAGE_NUMBER)
            {
                decryptPage(pageData, pageSize);
            }
        }
        else
        { // iochannel was not initialized, fall back to original method.
            super.readPage(pageNumber, pageData);
        }
    }


    /**
     *  Write a page from the supplied array.
     *
     *  <BR> MT - thread safe
     *
     *  @exception StandardException Standard Derby error policy
     *  @exception IOException IO error accessing page
     */
    protected void writePage(long pageNumber, byte[] pageData, boolean syncPage)
         throws IOException, StandardException
    {
        // If this is the first alloc page, there may be another thread
        // accessing the container information in the borrowed space on the
        // same page. In that case, we synchronize the entire method call, just
        // like RAFContainer.writePage() does, in order to avoid conflicts. For
        // all other pages it is safe to skip the synchronization, since
        // concurrent threads will access different pages and therefore don't
        // interfere with each other.
        if (pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
            synchronized (this) {
                writePage0(pageNumber, pageData, syncPage);
            }
        } else {
            writePage0(pageNumber, pageData, syncPage);
        }
    }

    private void writePage0(long pageNumber, byte[] pageData, boolean syncPage)
         throws IOException, StandardException
    {
        FileChannel ioChannel;
        synchronized (this) {
            // committed and dropped, do nothing.
            // This file container may only be a stub
            if (getCommittedDropState())
                return;
            ioChannel = getChannel();
        }

        if(ioChannel != null) {
            ///////////////////////////////////////////////////
            //
            // RESOLVE: right now, no logical -> physical mapping.
            // We can calculate the offset.  In the future, we may need to
            // look at the allocation page or the in memory translation table
            // to figure out where the page should go
            //
            /////////////////////////////////////////////////

            long pageOffset = pageNumber * pageSize;

            byte[] encryptionBuf = null;
            // We only need to allocate the encryptionBuf if updatePageArray is
            // actually going to use it.
            if (dataFactory.databaseEncrypted()) {
                encryptionBuf = new byte[pageSize];
            }

            byte[] dataToWrite = updatePageArray(pageNumber,
                                                 pageData,
                                                 encryptionBuf,
                                                 false);

            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(dataToWrite != null,
                        "RAFContainer4: dataToWrite is null after updatePageArray()");
            }

            ByteBuffer writeBuffer = ByteBuffer.wrap(dataToWrite);

            dataFactory.writeInProgress();
            try {
                if (SanityManager.DEBUG) {
                    synchronized(this) {
                        iosInProgress++;
                    }
                }

                writeFull(writeBuffer, ioChannel, pageOffset);
            } catch (ClosedChannelException ioe) {
                synchronized(this) {
                    /* If the write failed because the container has been closed
                     * for deletion between the start of this method and the
                     * write, we'll just ignore that, as this container is going
                     * away anyway.
                     * This could possibly happen if the Cache is cleaning this
                     * container while it is dropped - BaseDataFileFactory holds
                     * an exclusive lock on the container while dropping it to
                     * avoid other interference.
                     * See the getCommittedDropState() check at the top of this
                     * method.
                     */
                    if (getCommittedDropState()) {
                        if (SanityManager.DEBUG) {
                            SanityManager.DEBUG_PRINT("RAFContainer4",
                                "Write to a dropped and closed container discarded.");
                        }
                        return;
                    } else {
                        // This should not happen, better let the exception
                        // hurt where it's supposed to.
                        throw ioe;
                    }
                }
            } finally {
                if (SanityManager.DEBUG) {
                    synchronized(this) {
                        iosInProgress--;
                    }
                }

                dataFactory.writeFinished();
            }

            /* Note that the original "try {write} catch IOException { pad file,
             * write again }" in RAFContainer is removed here, because the
             * FileChannel Javadoc specifies that the file will be grown to
             * accommodate the new bytes.
             */

            if (syncPage) {
                dataFactory.writeInProgress();
                try{
                    if (SanityManager.DEBUG) {
                        synchronized(this) {
                            iosInProgress++;
                        }
                    }
                    if (!dataFactory.dataNotSyncedAtAllocation) {
                        ioChannel.force(false);
                    }
                } finally {
                    if (SanityManager.DEBUG) {
                        synchronized(this) {
                            iosInProgress--;
                        }
                    }
                    dataFactory.writeFinished();
                }
            } else {
                synchronized(this) {
                    needsSync = true;
                }
            }

        } else { // iochannel was not initialized, fall back to original method.
            super.writePage(pageNumber, pageData, syncPage);
        }
    }

    /**
     * Write a sequence of bytes at the given offset in a file.
     *
     * @param file the file to write to
     * @param bytes the bytes to write
     * @param offset the offset to start writing at
     * @throws IOException if an I/O error occurs while writing
     */
    void writeAtOffset(StorageRandomAccessFile file, byte[] bytes, long offset)
            throws IOException, StandardException
    {
        FileChannel ioChannel = getChannel(file);
        if (ioChannel != null) {
            writeFull(ByteBuffer.wrap(bytes), ioChannel, offset);
        } else {
            super.writeAtOffset(file, bytes, offset);
        }
    }

    /**
     * Read an embryonic page (that is, a section of the first alloc page that
     * is so large that we know all the borrowed space is included in it) from
     * the specified offset in a {@code StorageRandomAccessFile}.
     *
     * @param file the file to read from
     * @param offset where to start reading (normally
     * {@code FileContainer.FIRST_ALLOC_PAGE_OFFSET})
     * @return a byte array containing the embryonic page
     * @throws IOException if an I/O error occurs while reading
     * @throws StandardException if thread is interrupted.
     */
    byte[] getEmbryonicPage(StorageRandomAccessFile file, long offset)
            throws IOException, StandardException
    {
        FileChannel ioChannel = getChannel(file);
        if (ioChannel != null) {
            ByteBuffer buffer =
                    ByteBuffer.allocate(AllocPage.MAX_BORROWED_SPACE);
            readFull(buffer, ioChannel, offset);
            return buffer.array();
        } else {
            return super.getEmbryonicPage(file, offset);
        }
    }

    /**
     * Attempts to fill buf completely from start until it's full.
     * <p/>
     * FileChannel has no readFull() method, so we roll our own.
     * <p/>
     * @param dstBuffer buffer to read into
     * @param srcChannel channel to read from
     * @param position file position from where to read
     *
     * @throws IOException if an I/O error occurs while reading
     * @throws StandardException If thread is interrupted.
     */
    private final void readFull(ByteBuffer dstBuffer,
                                FileChannel srcChannel,
                                long position)
            throws IOException, StandardException
    {
        while(dstBuffer.remaining() > 0) {
            try {
                if (srcChannel.read(dstBuffer,
                                    position + dstBuffer.position()) == -1) {
                        throw new EOFException(
                            "Reached end of file while attempting to read a "
                            + "whole page.");
                }
            } catch (ClosedByInterruptException e) {
                throw StandardException.newException(
                    SQLState.FILE_IO_INTERRUPTED, e);
            }
        }
    }

    /**
     * Attempts to write buf completely from start until end, at the given
     * position in the destination fileChannel.
     * <p/>
     * FileChannel has no writeFull() method, so we roll our own.
     * <p/>
     * @param srcBuffer buffer to write
     * @param dstChannel channel to write to
     * @param position file position to start writing at
     *
     * @throws IOException if an I/O error occurs while writing
     * @throws StandardException If thread is interrupted.
     */
    private final void writeFull(ByteBuffer srcBuffer,
                                 FileChannel dstChannel,
                                 long position)
            throws IOException, StandardException
    {
        while(srcBuffer.remaining() > 0) {
            try {
                dstChannel.write(srcBuffer, position + srcBuffer.position());
            } catch (ClosedByInterruptException e) {
                throw StandardException.newException(
                    SQLState.FILE_IO_INTERRUPTED, e);
            }
        }
    }
}
