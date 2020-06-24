/*

   Derby - Class org.apache.derby.impl.io.vfmem.BlockedByteArray

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.impl.io.vfmem;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * Stores data in blocks, and supports reading/writing data from/into these
 * blocks.
 * <p>
 * The blocked array is expanded and shrunk as required.
 */
public class BlockedByteArray {

    /** Constant for 4 KB. */
    private static final int _4K = 4*1024;
    /** Constant for 8 KB. */
    private static final int _8K = 8*1024;
    /** Constant for 16 KB. */
    private static final int _16K = 16*1024;
    /** Constant for 32 KB. */
    private static final int _32K = 32*1024;
    /** The default block size. */
    private static final int DEFAULT_BLOCKSIZE = _4K;

    /** The default number of slots for holding a block of data. */
    private static final int INITIAL_BLOCK_HOLDER_SIZE = 1024;
    /**
     * Minimum number of holder slots to grow with when the block holder array
     * has to grow to be able to reference all the data arrays.
     */
    private static final int MIN_HOLDER_GROWTH = 1024;

    /** References to blocks of data. */
    private byte[][] blocks;
    /** The size of a block of data (the allocation unit). */
    private int blockSize;
    /** The number of allocated blocks. */
    private int allocatedBlocks;
    /** The number of bytes stored in the blocked byte array. */
    private long length;

    /**
     * Creates a new blocked byte array with the default number of slots to
     * hold byte arrays (blocks).
     * <p>
     * No blocks are pre-allocated.
     *
     * @see #INITIAL_BLOCK_HOLDER_SIZE
     */
    public BlockedByteArray() {
        blocks = new byte[INITIAL_BLOCK_HOLDER_SIZE][];
    }

    /**
     * Returns the byte at the given position.
     *
     * @param pos position to read from
     * @return A single byte.
     */
    public synchronized int read(long pos) {
        if (pos < length) {
            int block = (int)(pos / blockSize);
            int index = (int)(pos % blockSize);
            return (blocks[block][index] & 0xFF);
        }
        return -1;
    }

    /**
     * Reads up to {@code len} bytes.
     *
     * @param pos the position to start reading at
     * @param buf the destination buffer
     * @param offset offset into the destination buffer
     * @param len the number of bytes to read
     * @return The number of bytes read.
     */
     public synchronized int read(long pos, byte[] buf, int offset, int len) {
        // Due to the loop condition below, we have to check the length here.
        // The check is only required because calling code expects an exception.
        if (len < 0) {
            throw new ArrayIndexOutOfBoundsException(len);
        }
        // Check for EOF.
        if (pos >= length) {
            return -1;
        }
        // Adjust the length if required.
        len = (int)Math.min(len, length - pos);
        int block = (int)(pos / blockSize);
        int index = (int)(pos % blockSize);
        int read = 0;
        while (read < len) {
            int toRead = Math.min(len - read, blockSize - index);
            System.arraycopy(blocks[block], index, buf, offset + read, toRead);
            read += toRead;
            block++;
            index = 0;
        }
        return read;
     }

    /**
     * Returns the number of bytes allocated.
     *
     * @return Bytes allocated.
     */
    public synchronized long length() {
        return length;
    }

    /**
     * Changes the allocated length of the data.
     * <p>
     * If the new length is larger than the current length, the blocked byte
     * array will be extended with new blocks. If the new length is smaller,
     * existing (allocated) blocks will be removed if possible.
     *
     * @param newLength the new length of the allocated data in bytes
     */
    public synchronized void setLength(final long newLength) {
        // If capacity is requested before any writes has taken place.
//IC see: https://issues.apache.org/jira/browse/DERBY-4103
        if (blockSize == 0) {
            checkBlockSize((int)Math.min(Integer.MAX_VALUE, newLength));
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-5098
        final long currentCapacity = (long)allocatedBlocks * blockSize;
        if (newLength > currentCapacity) {
            // Allocate more blocks.
            increaseCapacity(newLength);
        } else if (newLength < currentCapacity) {
            if (newLength <= 0L) {
                // Just clear everything.
                allocatedBlocks = 0;
                blocks = new byte[INITIAL_BLOCK_HOLDER_SIZE][];
            } else {
                // Nullify the surplus data.
                int blocksToKeep = (int)(newLength / blockSize) +1;
                for (int i=blocksToKeep; i <= allocatedBlocks; i++) {
                    blocks[i] = null;
                }
                allocatedBlocks = Math.min(allocatedBlocks, blocksToKeep);
                // We keep the holder slots around, since the overhead for
                // doing so is pretty small.
            }
        }
        length = Math.max(0L, newLength);
    }

    /**
     * Writes the given bytes into the blocked byte array.
     *
     * @param pos the position to start writing at
     * @param buf the source buffer
     * @param offset the offset into the source buffer
     * @param len the number of bytes to write
     * @return The number of bytes written.
     */
    public synchronized int writeBytes(final long pos, final byte[] buf,
                                       int offset, final int len) {
        // Optimize block size if possible on first write.
        if (blockSize == 0) {
            checkBlockSize(len);
        }
        // Due to the loop condition below, we have to check the length here.
        // The check is only required because calling code expects an exception.
        if (len < 0) {
            throw new ArrayIndexOutOfBoundsException(len);
        }
        // Increase the capacity if required.
//IC see: https://issues.apache.org/jira/browse/DERBY-5098
        increaseCapacity(pos + len);
        // Calculate the block number and the index within this block.
        int block = (int)(pos / blockSize);
        int index = (int)(pos % blockSize);

        int written = 0;
        while (written < len) {
            int toWrite = Math.min(len - written, blockSize - index);
            System.arraycopy(buf, offset, blocks[block], index, toWrite);
            written += toWrite;
            offset += toWrite;
            if (written < len) {
                block++;
                index = 0;
            } else {
                index += toWrite;
            }
        }

        // Update the length if we wrote past the previous length.
        length = Math.max(length, pos + len);
        return written;
    }

    /**
     * Writes the given byte into the blocked byte array.
     *
     * @param pos the position to write the byte at
     * @param b the byte to write
     * @return {@code 1}, which is the number of bytes written.
     */
    public synchronized int writeByte(long pos, byte b) {
        // Optimize block size if possible on first write.
        if (blockSize == 0) {
            checkBlockSize(0);
        }
        // Increase the capacity if required.
//IC see: https://issues.apache.org/jira/browse/DERBY-5098
        increaseCapacity(pos);
        // Calculate the block number and the index within this block.
        int block = (int)(pos / blockSize);
        int index = (int)(pos % blockSize);
        blocks[block][index] = b;
        // Update the length if we wrote past the previous length.
        length = Math.max(length, pos +1);
        return 1; // The number of bytes written, always one.
    }

    /**
     * Returns an input stream serving the data in the blocked byte array.
     *
     * @return An {@code InputStream}-object.
     */
    synchronized BlockedByteArrayInputStream getInputStream() {
        return new BlockedByteArrayInputStream(this, 0L);
    }

    /**
     * Returns an output stream writing data into the blocked byte array.
     *
     * @param pos initial position of the output stream
     * @return An {@code OutputStream}-object.
     */
    synchronized BlockedByteArrayOutputStream getOutputStream(long pos) {
        if (pos < 0) {
            throw new IllegalArgumentException(
                                        "Position cannot be negative: " + pos);
        }
        return new BlockedByteArrayOutputStream(this, pos);
    }

    /**
     * Releases this array.
     */
    synchronized void release() {
        blocks = null;
        length = allocatedBlocks = -1;
    }

    /**
     * Tries to optimize the block size by setting it equal to the the page
     * size used by the database.
     * <p>
     * Since we don't have a way of knowing which page size will be used, wait
     * to set the block size until the first write request and see how many
     * bytes are written then.
     *
     * @param len the requested number of bytes to be written
     */
    private void checkBlockSize(int len) {
        // Optimize on the block size (if possible).
        if (len == _4K || len == _8K || len == _16K || len == _32K) {
            blockSize = len;
        } else {
            blockSize = DEFAULT_BLOCKSIZE;
        }
    }

    /**
     * Increases the capacity of this blocked byte array by allocating more
     * blocks.
     *
     * @param lastIndex the index that must fit into the array
     */
    //@GuardedBy("this")
    private void increaseCapacity(long lastIndex) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4103
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(blockSize > 0, "Invalid/unset block size");
        }
        // Safe-guard to avoid overwriting existing data.
//IC see: https://issues.apache.org/jira/browse/DERBY-5098
        if (lastIndex < (long)allocatedBlocks * blockSize) {
            return;
        }
        // Calculate required number of blocks, and create those lacking.
        // We may allocate one more array than required.
        final int blocksRequired = (int)((lastIndex) / blockSize) +1;
        if (blocksRequired > blocks.length) {
            // Grow the block holder array.
            // Make sure we have enough slots. Note that we only grow the block
            // holder array, we don't fill it with data blocks before needed.
            int growTo = Math.max(
                    // Grow at least ~33%.
//IC see: https://issues.apache.org/jira/browse/DERBY-4103
                    blocks.length + (blocks.length / 3),
                    // For cases where we need to grow more than 33%.
                    blocksRequired + MIN_HOLDER_GROWTH);
            byte[][] tmpBlocks = blocks;
            blocks = new byte[growTo][];
            // Copy the data array references.
            System.arraycopy(tmpBlocks, 0, blocks, 0, allocatedBlocks);
        }
        // Allocate new data arrays to accomodate lastIndex bytes.
        for (int i=allocatedBlocks; i < blocksRequired; i++) {
            blocks[i] = new byte[blockSize];
        }
        allocatedBlocks = blocksRequired;
    }
}
