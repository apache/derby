/*

   Derby - Class org.apache.derby.impl.services.replication.buffer.LogBufferElement

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

package org.apache.derby.impl.services.replication.buffer;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * ReplicationLogBuffer consists of n LogBufferElements, each of which
 * can store m log records in a single byte[].
 *
 * In addition to adding the log record information to the byte[], the
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
     * Append a single log record to this LogBufferElement.
     *
     * @param instant               the log address of this log record.
     * @param dataLength            number of bytes in data[]
     * @param dataOffset            offset in data[] to start copying from.
     * @param optionalDataLength    number of bytes in optionalData[]
     * @param optionalDataOffset    offset in optionalData[] to start copy from
     * @param data                  "from" array to copy "data" portion of rec
     * @param optionalData          "from" array to copy "optional data" from
     **/
    protected void appendLogRecord(long instant,
                                int dataLength,
                                int dataOffset,
                                int optionalDataLength,
                                int optionalDataOffset,
                                byte[] data,
                                byte[] optionalData){

        if (SanityManager.DEBUG){
            int totalSize = dataLength + optionalDataLength +
                ReplicationLogBuffer.LOG_RECORD_FIXED_OVERHEAD_SIZE;
            SanityManager.ASSERT(freeSize() >= totalSize,
                                 "Log record does not fit into"+
                                 " this LogBufferElement");
        }

        position = appendLong(instant, position);
        position = appendInt(dataLength, position);
        position = appendInt(dataOffset, position);
        position = appendInt(optionalDataLength, position);
        position = appendInt(optionalDataOffset, position);

        if (dataLength > 0){
            position = appendBytes(data, position, dataLength);
        }

        if (optionalDataLength > 0) {
            position = appendBytes(optionalData, position, optionalDataLength);
        }

        this.greatestInstant = instant;
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
     * @return new position
     */
    private int appendBytes(byte b[], int pos, int length) {
        if (SanityManager.DEBUG){
            SanityManager.ASSERT(freeSize() >= (pos+length),
                                 "byte[] is to big to fit"+
                                 " into this buffer");
            SanityManager.ASSERT(b != null, "Cannot append null to buffer");
        }
        System.arraycopy(b, 0, bufferdata, pos, length);
        return pos + length;
    }

    /**
     * Append an int to this LogBufferElement.
     * @return new position
     */
    private int appendInt(int i, int p) {
        bufferdata[p++] = (byte) (i >> 24);
        bufferdata[p++] = (byte) (i >> 16);
        bufferdata[p++] = (byte) (i >> 8);
        bufferdata[p++] = (byte) i;
        return p;
    }

    /**
     * Append a long to this LogBufferElement.
     * @return new position
     */
    private int appendLong(long l, int p) {
        bufferdata[p++] = (byte) (l >> 56);
        bufferdata[p++] = (byte) (l >> 48);
        bufferdata[p++] = (byte) (l >> 40);
        bufferdata[p++] = (byte) (l >> 32);
        bufferdata[p++] = (byte) (l >> 24);
        bufferdata[p++] = (byte) (l >> 16);
        bufferdata[p++] = (byte) (l >> 8);
        bufferdata[p++] = (byte) l;
        return p;
    }

}
