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
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.util.InterruptStatus;
import org.apache.derby.iapi.util.InterruptDetectedException;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.AsynchronousCloseException;
import org.apache.derby.io.StorageRandomAccessFile;

/**
 * RAFContainer4 overrides a few methods in FileContainer/RAFContainer in order
 * to use FileChannel from Java 1.4's New IO framework to issue multiple IO
 * operations to the same file concurrently instead of strictly serializing IO
 * operations using a mutex on the container object. Since we compile with Java
 * 1.4, the override "annotations" are inside the method javadoc headers.
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

    private final Object channelCleanupMonitor = new Object();

    // channelCleanupMonitor protects next three state variables:

    // volatile on threadsInPageIO, is just to ensure that we get a correct
    // value for debugging: we can't always use channelCleanupMonitor
    // then. Otherwise protected by channelCleanupMonitor. Debugging value not
    // safe on 1.4, but who cares..
    private volatile int threadsInPageIO = 0;

    // volatile on restoreChannelInProgress: corner case where we can't use
    // channelCleanupMonitor: the corner case should not happen if NIO works as
    // specified: thats is, uniquely only one thread sees
    // ClosedByInterruptException, always.  Unfortunately, we sometimes get
    // AsynchronousCloseException, which another thread could theoretically
    // also see it it were interrupted at the same time inside NIO. In this
    // case, we could get two threads competing to do recovery. This is
    // normally OK, unless the thread owns allocCache or "this", in which case
    // we risk dead-lock if we synchronize on restoreChannelInProgress
    // (explained below). So, we have to rely on volatile, which isn't safe in
    // Java 1.4 (old memory model),
    private volatile boolean restoreChannelInProgress = false;


    // In case the recovering thread can't successfully recover the container
    // for some reason, it will throw, so other waiting threads need to give up
    // as well.
    private boolean giveUpIO = false;
    private final Object giveUpIOm = new Object(); // its monitor

/**
     * For debugging - will be incremented when an IO is started, decremented
     * when it is done. Should be == 0 when container state is changed.
     */
    private int iosInProgress = 0; // protected by monitor on "this"

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

    private ContainerKey currentIdentity;

    /*
     * Wrapping methods that retrieve the FileChannel from RAFContainer's
     * fileData after calling the real methods in RAFContainer.
     *
     * override of RAFContainer#openContainer
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

        currentIdentity = newIdentity;
        return super.openContainer(newIdentity);
    }

    /**
     * override of RAFContainer#createContainer
     */
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

        currentIdentity = newIdentity;
        super.createContainer(newIdentity);
    }

    /**
     * When the existing channel ({@code ourChannel}) has been closed due to
     * interrupt, we need to reopen the underlying RAF to get a fresh channel
     * so we can resume IO.
     */
    private void reopen() throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!ourChannel.isOpen());
        }
        ourChannel = null;
        reopenContainer(currentIdentity);
    }

    /**
     * override of RAFContainer#closeContainer
     */
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
     *  <p/>
     *  override of RAFContainer#readPage
     *  <p/>
     *  <BR> MT - thread safe
     *  @exception IOException exception reading page
     *  @exception StandardException Standard Derby error policy
     */
    protected void readPage(long pageNumber, byte[] pageData)
         throws IOException, StandardException
    {
        readPage(pageNumber, pageData, -1L);
    }


    /**
     *  Read a page into the supplied array.
     *  <p/>
     *  override of RAFContainer#readPage
     *  <p/>
     *  <BR> MT - thread safe

     *  @param pageNumber the page number to read data from, or -1 (called from
     *                    getEmbryonicPage)
     *  @param pageData  the buffer to read data into
     *  @param offset -1 normally (not used since offset is computed from
     *                   pageNumber), but used if pageNumber == -1
     *                   (getEmbryonicPage)
     *  @exception IOException exception reading page
     *  @exception StandardException Standard Derby error policy
     */
    private void readPage(long pageNumber, byte[] pageData, long offset)
         throws IOException, StandardException
    {
        // Interrupt recovery "stealthMode": If this thread holds a monitor on
        //
        //   a) "this" (when RAFContainer#clean calls getEmbryonicPage via
        //       writeRAFHEader) or
        //   b) "allocCache" (e.g. FileContainer#newPage,
        //       #pageValid)
        //
        // we cannot grab channelCleanupMonitor lest another thread is one
        // doing recovery, since the recovery thread will try to grab both
        // those monitors during container recovery.  So, just forge ahead
        // in stealth mode (i.e. the recovery thread doesn't see us). If we see
        // ClosedChannelException, throw InterruptDetectedException, so we can
        // retry from RAFContainer releasing "this", or FileContainer
        // (releasing allocCache) as the case may be, so the recovery thread
        // can do its thing.

        final boolean holdsThis = Thread.holdsLock(this);
        final boolean holdsAllocCache = Thread.holdsLock(allocCache);

        final boolean stealthMode = holdsThis || holdsAllocCache;

        if (SanityManager.DEBUG) {
            // getEmbryonicPage only
            if (pageNumber == -1) {
                if (!holdsThis) {
                    // Remove when DERBY-6354 is closed:
                    new Throwable().printStackTrace(SanityManager.GET_DEBUG_STREAM());
                }
                SanityManager.ASSERT(holdsThis);
            }
            if (holdsThis) {
                SanityManager.ASSERT(pageNumber == -1);
            }
        }


        if (stealthMode) {
            // We go into stealth mode. If we see an
            // CloseChannelExceptionexception, we will get out of here anyway,
            // so we don't need to increment threadsInPageIO (nor can we,
            // without risking dead-lock),
        } else {
            synchronized (channelCleanupMonitor) {

                // Gain entry
                int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;

                while (restoreChannelInProgress) {
                    if (retries-- == 0) {
                        throw StandardException.newException(
                            SQLState.FILE_IO_INTERRUPTED);
                    }

                    try {
                        channelCleanupMonitor.wait(
                            InterruptStatus.INTERRUPT_RETRY_SLEEP);
                    } catch (InterruptedException e) {
                        InterruptStatus.setInterrupted();
                    }

                }

                threadsInPageIO++;
            }
        }


        boolean success = false;
        int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;

      try {
        while (!success) {
            try {
                if (pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
                    // If this is the first alloc page, there may be another
                    // thread accessing the container information in the
                    // borrowed space on the same page. In that case, we
                    // synchronize the entire method call, just like
                    // RAFContainer.readPage() does, in order to avoid
                    // conflicts. For all other pages it is safe to skip the
                    // synchronization, since concurrent threads will access
                    // different pages and therefore don't interfere with each
                    // other:
                    synchronized (this) {
                        readPage0(pageNumber, pageData, offset);
                    }
                } else {
                    // Normal case.
                    readPage0(pageNumber, pageData, offset);
                }

                success = true;

            } catch (ClosedChannelException e) {
                handleClosedChannel(e, stealthMode, retries--);
            }
        }
      } finally {
        if (stealthMode) {
            // don't touch threadsInPageIO
        } else {
            synchronized (channelCleanupMonitor) {
                threadsInPageIO--;
            }
        }
      }
    }

    private void readPage0(long pageNumber, byte[] pageData, long offset)
         throws IOException, StandardException
    {
        FileChannel ioChannel;
        synchronized (this) {
            if (SanityManager.DEBUG) {
                if (pageNumber != -1L) {
                    SanityManager.ASSERT(!getCommittedDropState());
                } // else: can happen from getEmbryonicPage
            }
            ioChannel = getChannel();
        }

        if (SanityManager.DEBUG) {
            if (pageNumber == -1L || pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
                // can happen from getEmbryonicPage
                SanityManager.ASSERT(Thread.holdsLock(this));
            } else {
                SanityManager.ASSERT(!Thread.holdsLock(this));
            }
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

                if (offset == -1L) {
                    // Normal page read doesn't specify offset,
                    // so use one computed from page number.
                    readFull(pageBuf, ioChannel, pageOffset);
                } else {
                    // getEmbryonicPage specifies it own offset, so use that
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(pageNumber == -1L);
                    }

                    readFull(pageBuf, ioChannel, offset);
                }
            }
            finally {
                if (SanityManager.DEBUG) {
                    synchronized(this) {
                        iosInProgress--;
                    }
                }

            }

            if (dataFactory.databaseEncrypted() &&
                pageNumber != FIRST_ALLOC_PAGE_NUMBER &&
                pageNumber != -1L /* getEmbryonicPage */)
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
     *  <p/>
     *  override of RAFContainer#writePage
     *  <p/>
     *  <BR> MT - thread safe
     *
     *  @exception StandardException Standard Derby error policy
     *  @exception IOException IO error accessing page
     */
    protected void writePage(long pageNumber, byte[] pageData, boolean syncPage)
         throws IOException, StandardException
    {
        // Interrupt recovery "stealthMode": If this thread holds a monitor on
        //
        //   a) "allocCache" (e.g. FileContainer#newPage, #pageValid),
        //
        // we cannot grab channelCleanupMonitor lest another thread is one
        // doing recovery, since the recovery thread will try to grab both
        // those monitors during container recovery.  So, just forge ahead
        // in stealth mode (i.e. the recovery thread doesn't see us). If we see
        // ClosedChannelException, throw InterruptDetectedException, so we can
        // retry from FileContainer releasing allocCache, so the recovery
        // thread can do its thing.
        boolean stealthMode = Thread.holdsLock(allocCache);

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!Thread.holdsLock(this));
        }

       if (stealthMode) {
            // We go into stealth mode. If we see an
            // CloseChannelExceptionexception, we will get out of here anyway,
            // so we don't need to increment threadsInPageIO (nor can we,
            // without risking dead-lock),
        } else {
            synchronized (channelCleanupMonitor) {

                // Gain entry
                int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;

                while (restoreChannelInProgress) {
                    if (retries-- == 0) {
                        throw StandardException.newException(
                            SQLState.FILE_IO_INTERRUPTED);
                    }

                    try {
                        channelCleanupMonitor.wait(
                            InterruptStatus.INTERRUPT_RETRY_SLEEP);
                    } catch (InterruptedException e) {
                        InterruptStatus.setInterrupted();
                    }

                }

                threadsInPageIO++;
            }
        }

        boolean success = false;
        int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;

      try {
        while (!success) {
            try {
                if (pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
                    // If this is the first alloc page, there may be
                    // another thread accessing the container information
                    // in the borrowed space on the same page. In that
                    // case, we synchronize the entire method call, just
                    // like RAFContainer.writePage() does, in order to
                    // avoid conflicts. For all other pages it is safe to
                    // skip the synchronization, since concurrent threads
                    // will access different pages and therefore don't
                    // interfere with each other.
                    synchronized (this) {
                        writePage0(pageNumber, pageData, syncPage);
                    }
                } else {
                    writePage0(pageNumber, pageData, syncPage);
                }

                success = true;

            } catch (ClosedChannelException e) {
                handleClosedChannel(e, stealthMode, retries--);
            }
        }
      } finally {
        if (stealthMode) {
            // don't touch threadsInPageIO
        } else {
            synchronized (channelCleanupMonitor) {
                threadsInPageIO--;
            }
        }
      }
    }

    /**
     * <p>
     * This method handles what to do when, during a NIO operation we receive a
     * {@code ClosedChannelException}. Note the specialization hierarchy:
     * </p>
     * <p>
     * {@code ClosedChannelException} -&gt; {@code AsynchronousCloseException} -&gt;
     * {@code ClosedByInterruptException}
     * </p>
     * <p>
     * If {@code e} is a ClosedByInterruptException, we normally start
     * container recovery, i.e. we need to reopen the random access file so we
     * get get a new interruptible channel and continue IO.
     * </p>
     * <p>
     * If {@code e} is a {@code AsynchronousCloseException} or a plain {@code
     * ClosedChannelException}, the behavior depends of {@code stealthMode}:
     * </p>
     * <p>
     * If {@code stealthMode == false}, the method will wait for
     * another thread tp finish recovering the IO channel before returning.
     * </p>
     * <p>
     * If {@code stealthMode == true}, the method throws {@code
     * InterruptDetectedException}, allowing retry at a higher level in the
     * code.  The reason for this is that we sometimes need to release monitors
     * on objects needed by the recovery thread.
     * </p>
     *
     * @param e Should be an instance of {@code ClosedChannelException}.
     * @param stealthMode If {@code true}, do retry at a higher level
     * @param retries Give up waiting for another thread to reopen the channel
     *                when {@code retries} reaches 0. Only applicable if {@code
     *                stealthMode == false}.
     * @throws InterruptDetectedException if retry at higher level is required
     *         {@code stealthMode == true}.
     * @throws StandardException standard error policy, incl. when we give up
     *                           waiting for another thread to reopen channel
     */
    private void handleClosedChannel(ClosedChannelException e,
                                     boolean stealthMode,
                                     int retries)
            throws StandardException {

        // if (e instanceof ClosedByInterruptException e) {
        // Java NIO Bug 6979009:
        // http://bugs.sun.com/view_bug.do?bug_id=6979009
        // Sometimes NIO throws AsynchronousCloseException instead of
        // ClosedByInterruptException

        if (e instanceof AsynchronousCloseException) {
            // Subsumes ClosedByInterruptException

            // The interrupted thread may or may not get back here to try
            // recovery before other concurrent IO threads will see (the
            // secondary) ClosedChannelException, but we have logic to handle
            // that, cf threadsInPageIO.

            if (Thread.currentThread().isInterrupted()) {
                if (recoverContainerAfterInterrupt(
                            e.toString(),
                            stealthMode)) {
                    return; // do I/O over again
                }
            }

            // Recovery is in progress, wait for another interrupted thread to
            // clean up.

            awaitRestoreChannel(e, stealthMode);
        } else {
            // According to the exception type, We are not the thread that
            // first saw the channel interrupt, so no recovery attempt.
            InterruptStatus.noteAndClearInterrupt(
                "ClosedChannelException",
                threadsInPageIO,
                hashCode());

            awaitRestoreChannel(e, stealthMode);
            if (retries == 0) {
                throw StandardException.newException(
                    SQLState.FILE_IO_INTERRUPTED);
            }
        }
    }

    /**
     * Use when seeing an exception during IO and when another thread is
     * presumably doing the recovery.
     * <p/>
     * If {@code stealthMode == false}, wait for another thread to recover the
     * container after an interrupt. If {@code stealthMode == true}, throw
     * internal exception {@code InterruptDetectedException} to do retry from
     * higher in the stack.
     * <p/>
     * If {@code stealthMode == false}, maximum wait time for the container to
     * become available again is determined by the product {@code
     * InterruptStatus.MAX_INTERRUPT_RETRIES *
     * InterruptStatus.INTERRUPT_RETRY_SLEEP}.
     * There is a chance this thread will not see any recovery occuring (yet),
     * in which case it waits for a bit and just returns, so the caller must
     * retry IO until success.
     * <p/>
     * If for some reason the recovering thread has given up on resurrecting
     * the container, cf {@code #giveUpIO}, the method throws {@code
     * FILE_IO_INTERRUPTED}.
     *
     * @param e the exception we saw during IO
     * @param stealthMode true if the thread doing IO in stealth mode
     *
     * @throws StandardException {@code InterruptDetectedException} and normal
     *                            error policy
     */
    private void awaitRestoreChannel (Exception e,
                                      boolean stealthMode)
            throws StandardException {

        if (stealthMode) {
            // Retry handled at FileContainer or RAFContainer level
            //
            // This is necessary since recovery needs the monitor on allocCache
            // or "this" to clean up, so we need to back out all the way so
            // this thread can release the monitor to allow recovery to
            // proceed.
            if (SanityManager.DEBUG) {
                    debugTrace(
                        "thread does stealth mode retry");
            }

            synchronized (giveUpIOm) {
                if (giveUpIO) {

                    if (SanityManager.DEBUG) {
                        debugTrace(
                            "giving up retry, another thread gave up " +
                            "resurrecting container ");
                    }

                    throw StandardException.newException(
                        SQLState.FILE_IO_INTERRUPTED);
                }
            }

            throw new InterruptDetectedException();
        }

        synchronized (channelCleanupMonitor) {
            // Pave way for the thread that received the interrupt that caused
            // the channel close to clean up, by signaling we are waiting (no
            // longer doing IO):

            threadsInPageIO--;
        }

        // Wait here till the interrupted thread does container recovery.
        // If we get a channel exception for some other reason, this will never
        // happen, so throw after waiting long enough (60s).

        int timesWaited = -1;

        while (true) {
            synchronized(channelCleanupMonitor) {
                while (restoreChannelInProgress) {
                    timesWaited++;

                    if (SanityManager.DEBUG) {
                        debugTrace(
                            "thread needs to wait for container recovery: " +
                            "already waited " + timesWaited + " times");
                    }

                    if (timesWaited > InterruptStatus.MAX_INTERRUPT_RETRIES) {
                        // Max, give up, probably way too long anyway,
                        // but doesn't hurt?
                        throw StandardException.newException(
                            SQLState.FILE_IO_INTERRUPTED, e);
                    }

                    try {
                        channelCleanupMonitor.wait(
                            InterruptStatus.INTERRUPT_RETRY_SLEEP);
                    } catch (InterruptedException we) {
                        InterruptStatus.setInterrupted();
                    }
                }

                // Since the channel is presumably ok (lest giveUpIO is set,
                // see below), we put ourselves back in the IO set of threads:

                threadsInPageIO++;
                break;
            }
        }

        synchronized (giveUpIOm) {
            if (giveUpIO) {

                if (SanityManager.DEBUG) {
                    debugTrace(
                        "giving up retry, another thread gave up " +
                        "resurrecting container ");
                }

                threadsInPageIO--;
                throw StandardException.newException(
                    SQLState.FILE_IO_INTERRUPTED);
            }
        }

        if (timesWaited == -1) {
            // We have not seen restoreChannelInProgress, so we may
            // have raced past the interrupted thread, so let's wait a
            // bit before we attempt a new I/O.
            try {
                Thread.sleep(InterruptStatus.INTERRUPT_RETRY_SLEEP);
            } catch (InterruptedException we) {
                // This thread is getting hit, too..
                InterruptStatus.setInterrupted();
            }
        }
    }


    /**
     * Use this when the thread has received a ClosedByInterruptException (or,
     * prior to JDK 1.7 it may also be AsynchronousCloseException - a bug)
     * exception during IO and its interruped flag is also set. This makes this
     * thread a likely candicate to do container recovery, unless another
     * thread started it already, cf. return value.
     *
     * @param whence caller site (debug info)
     * @param stealthMode don't update threadsInPageIO if true
     * @return true if we did recovery, false if we saw someone else do it and
     * abstained
     */
    private boolean recoverContainerAfterInterrupt(
        String whence,
        boolean stealthMode) throws StandardException {

        if (stealthMode && restoreChannelInProgress) {
            // 1) Another interrupted thread got to do the cleanup before us, so
            // yield.
            // This should not happen, but since
            // we had to "fix" NIO, cf. the code marked (**), we could
            // theoretically see two:
            //
            // - the thread that got AsynchronousCloseException, but was the
            //   one that caused the channel close: it will decide (correctly)
            //   it is the one to do recovery.
            //
            // - another thread that got an interrupt after doing successful IO
            //   but seeing a closed channel: it will decide (incorrectly) it
            //   is the one to do recovery. But since we had to fix NIO, this
            //   case gets conflated with the case that this was *really* the
            //   thread the caused the channel close.
            //
            // Not safe for Java 1.4 (only volatile protection for
            // restoreChannelInProgress here), compare safe test below (not
            // stealthMode).
            //
            // 2) The other way to end up here is if we get interrupted during
            // getEmbryonicPage called during container recovery from the same
            // thread (restoreChannelInProgress is set then, and
            // getEmbryonicPage is stealthMode)

            InterruptStatus.noteAndClearInterrupt(
                whence,
                threadsInPageIO,
                hashCode());

            return false;
        }

        synchronized (channelCleanupMonitor) {
            if (restoreChannelInProgress) {
                // Another interrupted thread got to do the cleanup before us,
                // so yield, see above explanation.
                InterruptStatus.noteAndClearInterrupt(
                    whence,
                    threadsInPageIO,
                    hashCode());

                return false;
            }

            if (stealthMode) {
                // don't touch threadsInPageIO
            } else {
                threadsInPageIO--;
            }

            // All new writers will now wait till we're done, see "Gain entry"
            // in writePage above. Any concurrent threads already inside will
            // also wait till we're done, see below
            restoreChannelInProgress = true;
        }

        // Wait till other concurrent threads hit the wall
        // (ClosedChannelException) and are a ready waiting for us to clean up,
        // so we can set them loose when we're done.
        int retries = InterruptStatus.MAX_INTERRUPT_RETRIES;

        while (true) {
            synchronized (channelCleanupMonitor) {
                if (threadsInPageIO == 0) {
                    // Either no concurrent threads, or they are now waiting
                    // for us to clean up (see ClosedChannelException case)
                    break;
                }

                if (retries-- == 0) {
                    // Clean up state and throw
                    restoreChannelInProgress = false;
                    channelCleanupMonitor.notifyAll();

                    throw StandardException.newException(
                        SQLState.FILE_IO_INTERRUPTED);
                }
            }

            try {
                Thread.sleep(InterruptStatus.INTERRUPT_RETRY_SLEEP);
            } catch (InterruptedException te) {
                InterruptStatus.setInterrupted();
            }
        }


        // Initiate recovery
        synchronized (channelCleanupMonitor) {
            try {
                InterruptStatus.noteAndClearInterrupt(
                    whence, threadsInPageIO, hashCode());

                synchronized(this) {
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(ourChannel != null,
                                             "ourChannel is null");
                        SanityManager.ASSERT(!ourChannel.isOpen(),
                                             "ourChannel is open");
                    }
                }

                while (true) {
                    synchronized(this) {
                        try {
                            reopen();
                        } catch (Exception newE) {
                            // Something else failed - shutdown happening?
                            synchronized(giveUpIOm) {
                                // Make sure other threads will give up and
                                // throw, too.
                                giveUpIO = true;

                                if (SanityManager.DEBUG) {
                                    debugTrace(
                                        "can't resurrect container: " +
                                        newE);
                                }

                                throw StandardException.newException(
                                    SQLState.FILE_IO_INTERRUPTED, newE);
                            }
                        }
                        break;
                    }
                }

                if (stealthMode) {
                    // don't touch threadsInPageIO
                } else {
                    threadsInPageIO++;
                }

                // retry IO
            } finally {
                // Recovery work done (or failed), now set other threads free
                // to retry or give up as the case may be, cf. giveUpIO.
                restoreChannelInProgress = false;
                channelCleanupMonitor.notifyAll();
            }
        } // end channelCleanupMonitor region

        return true;
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

        if (SanityManager.DEBUG) {
            if (pageNumber == FIRST_ALLOC_PAGE_NUMBER) {
                // page 0
                SanityManager.ASSERT(Thread.holdsLock(this));
            } else {
                SanityManager.ASSERT(!Thread.holdsLock(this));
            }
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
                            debugTrace(
                                "write to a dropped and " +
                                "closed container discarded.");
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

        } else {
            // iochannel was not initialized, fall back to original method.
            super.writePage(pageNumber, pageData, syncPage);
        }
    }

    /**
     * Write a sequence of bytes at the given offset in a file.  This method
     * operates in <em>stealth mode</em>, see doc for {@link
     * #handleClosedChannel handleClosedChannel}.
     * This presumes that IO retry happens at a higher level, i.e. the
     * caller(s) must be prepared to handle {@code InterruptDetectedException}.
     * <p/>
     * This method overrides FileContainer#writeAtOffset.
     * <p/>
     * @param file the file to write to
     * @param bytes the bytes to write
     * @param offset the offset to start writing at
     * @throws IOException if an I/O error occurs while writing
     */
    void writeAtOffset(StorageRandomAccessFile file, byte[] bytes, long offset)
            throws IOException, StandardException
    {
        FileChannel ioChannel = getChannel(file);

        if (ioChannel == null) {
            super.writeAtOffset(file, bytes, offset);
            return;
        }

        ourChannel = ioChannel;

        boolean success = false;
        final boolean stealthMode = true;

        while (!success) {

            synchronized (this) {
                // don't use ourChannel directly, could need re-initilization
                // after interrupt and container reopening:
                ioChannel = getChannel();
            }

            try {
                writeFull(ByteBuffer.wrap(bytes), ioChannel, offset);
                success = true;
            } catch (ClosedChannelException e) {
                handleClosedChannel(e, stealthMode, -1 /* NA */);
            }
        }
    }

    /**
     * Read an embryonic page (that is, a section of the first alloc page that
     * is so large that we know all the borrowed space is included in it) from
     * the specified offset in a {@code StorageRandomAccessFile}.
     * <p/>
     * override of FileContainer#getEmbryonicPage
     * <p/>
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
            byte[] buffer = new byte[AllocPage.MAX_BORROWED_SPACE];
            readPage(-1L, buffer, offset);
            return buffer;
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
    private void readFull(ByteBuffer dstBuffer,
                          FileChannel srcChannel,
                          long position)
            throws IOException, StandardException
    {
        while(dstBuffer.remaining() > 0) {
            if (srcChannel.read(dstBuffer,
                                    position + dstBuffer.position()) == -1) {
                throw new EOFException(
                    "Reached end of file while attempting to read a "
                    + "whole page.");
            }

            // (**) Sun Java NIO is weird: it can close the channel due to an
            // interrupt without throwing if bytes got transferred. Compensate,
            // so we can clean up.  Bug 6979009,
            // http://bugs.sun.com/view_bug.do?bug_id=6979009
            if (Thread.currentThread().isInterrupted() &&
                    !srcChannel.isOpen()) {
                throw new ClosedByInterruptException();
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
    private void writeFull(ByteBuffer srcBuffer,
                           FileChannel dstChannel,
                           long position)
            throws IOException
    {
        while(srcBuffer.remaining() > 0) {
            dstChannel.write(srcBuffer, position + srcBuffer.position());

            // (**) Sun JAVA NIO is weird: it can close the channel due to an
            // interrupt without throwing if bytes got transferred. Compensate,
            // so we can clean up. Bug 6979009,
            // http://bugs.sun.com/view_bug.do?bug_id=6979009
            if (Thread.currentThread().isInterrupted() &&
                    !dstChannel.isOpen()) {
                throw new ClosedByInterruptException();
            }
        }
    }

    private static void debugTrace (String msg) {
        if (SanityManager.DEBUG) { // redundant, just to remove code in insane
            if (SanityManager.DEBUG_ON("RAF4")) {
                SanityManager.DEBUG_PRINT(
                    "RAF4",
                    Thread.currentThread().getName() + " " + msg);
            }
        }
    }
}
