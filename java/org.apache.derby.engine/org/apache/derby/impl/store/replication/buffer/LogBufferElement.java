/*

   Derby - Class org.apache.derby.impl.store.replication.buffer.LogBufferElement

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

package org.apache.derby.impl.store.replication.buffer;

import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * ReplicationLogBuffer consists of n LogBufferElements, each of which
 * can store a number of log records in a single byte[].
 * <p>
 * The format of each log record in the LogBufferElement is the same
 * as is written to log file in LogAccessFile:<br>
 *
 * (int)    total_length (data[].length + optionaldata[].length)<br>
 * (long)   instant<br>
 * (byte[]) data+optionaldata<br>
 * (int)    total_length<br>
 *
 * </p>
 * In addition to adding a chunk of log records to the byte[], the
 * greatestInstant variable is updated for every append so that
 * getLastInstant can be used to get the highest log instant in this
 * LogBufferElement.
 */

class LogBufferElement {

    private final byte[] bufferdata;
    private int position;
    private long greatestInstant;
    // put back in freeBuffers when content has been sent to slave?
    private boolean recycleMe;

    protected LogBufferElement(int bufferSize){
        bufferdata = new byte[bufferSize];
        init();
    }

    /**
     * Resets all variables to default values. Should be called before
     * a LogBufferElement is reused.
     */
    protected void init() {
        this.position = 0;
        greatestInstant = 0;
        recycleMe = true; //always recycle unless explicitly told otherwise
    }

    /**
     * Append a chunk of log records to this LogBufferElement.
     *
     * @param greatestInstant   the instant of the log record that was
     *                          added last to this chunk of log
     * @param log               the chunk of log records
     * @param logOffset         offset in log to start copy from
     * @param logLength         number of bytes to copy, starting
     *                          from logOffset
     **/
    protected void appendLog(long greatestInstant,
                             byte[] log, int logOffset, int logLength) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2977

        if (SanityManager.DEBUG){
            SanityManager.ASSERT(freeSize() >= logLength,
                                 "Log chunk does not fit into"+
                                 " this LogBufferElement");
        }

        this.greatestInstant = greatestInstant;
        position = appendBytes(log, logOffset, position, logLength);
    }

    /**
     * @return A byte[] representation of the log records appended to
     * this LogBufferElement
     */
    protected byte[] getData(){
        return bufferdata;
    }

    /**
     * @return The highest log instant appended to this LogBufferElement
     */
    protected long getLastInstant(){
        return greatestInstant;
    }

    /**
     * @return Number of unused bytes in this LogBufferElement
     */
    protected int freeSize(){
        return bufferdata.length - position;
    }

    /**
     * @return Number of used bytes in this LogBufferElement
     */
    protected int size(){
        return position;
    }

    /**
     * @return true if this LogBufferElement should be reused, i.e.
     * added to freeBuffers after being consumed.
     */
    protected boolean isRecyclable(){
        return recycleMe;
    }

    protected void setRecyclable(boolean r){
        recycleMe = r;
    }

    /*
     * The append methods should be changed to use java.nio.ByteBuffer
     * if it is decided that replication will never use j2me. We use
     * our own implementation for now so that j2me is not blocked.
     */

    /**
     * Append a byte[] to this LogBufferElement.
     *
     * @param b       where the bytes are copied from
     * @param offset  offset in b to start copying from
     * @param pos     the position in this LogBufferElement to start copying to
     * @param length  number of bytes to copy from b, starting from offset
     *
     * @return new position
     */
    private int appendBytes(byte b[], int offset, int pos, int length) {
        if (SanityManager.DEBUG){
//IC see: https://issues.apache.org/jira/browse/DERBY-2977
            SanityManager.ASSERT(freeSize() >= length,
                                 "byte[] is to big to fit"+
                                 " into this buffer");
            SanityManager.ASSERT(b != null, "Cannot append null to buffer");
        }
        System.arraycopy(b, offset, bufferdata, pos, length);
        return pos + length;
    }

}
