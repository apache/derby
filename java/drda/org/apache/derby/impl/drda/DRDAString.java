/*
 * Derby - class org.apache.derby.impl.drda.DRDAString
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.derby.impl.drda;

/**
 * This class provides functionality for reusing buffers and strings
 * when parsing DRDA packets. A byte array representing a string is
 * stored internally. When the string is requested as a
 * <code>String</code> object, the byte array is converted to a
 * string, and the string is cached to avoid unnecessary conversion
 * later.
 */
final class DRDAString {
    /** Buffer representing the string. */
    private byte[] buffer;
    
    /** Keep the DDMWriter as it contains the current CCSID manager being used */
    private final DDMWriter writer;

    /** True if the contents were modified in the previous call to
     * <code>setBytes</code>. */
    private boolean modified;

    /** The previously generated string. */
    private String cachedString;

    /**
     * Create a new <code>DRDAString</code> instance.
     *
     * @param w a <code>DDMWriter</code> which holds current CCSidManager
     * and which encoding is used
     */
    DRDAString(DDMWriter w) {
        this.buffer = new byte[0];
        this.writer = w;
        this.cachedString = null;
    }

    /**
     * Check whether the internal buffer contains the same data as
     * another byte buffer.
     *
     * @param buf a byte array
     * @param offset start position in the byte array
     * @param size how many bytes to read from the byte array
     * @return <code>true</code> if the internal buffer contains the
     * same data as the specified byte array
     */
    private boolean equalTo(byte[] buf, int offset, int size) {
        int len = buffer.length;
        if (len != size) return false;
        for (int i = 0; i < len; ++i) {
            if (buffer[i] != buf[i+offset]) return false;
        }
        return true;
    }

    /**
     * Modify the internal byte buffer. If the new data is equal to
     * the old data, the cached values are not cleared.
     *
     * @param src the new bytes
     * @param offset start offset
     * @param size number of bytes to use
     */
    public void setBytes(byte[] src, int offset, int size) {
        if (equalTo(src, offset, size)) {
            modified = false;
            return;
        }
        if (buffer.length != size) {
            buffer = new byte[size];
        }
        System.arraycopy(src, offset, buffer, 0, size);
        modified = true;
        cachedString = null;
    }

    /**
     * Check whether the contents of the <code>DRDAString</code> were
     * modified in the previous call to <code>setBytes()</code>.
     *
     * @return <code>true</code> if the contents were modified
     */
    public boolean wasModified() {
        return modified;
    }

    /**
     * Convert the internal byte array to a string. The string value
     * is cached.
     *
     * @return a <code>String</code> value
     */
    public String toString() {
        if (cachedString == null) {
            cachedString =
                writer.getCurrentCcsidManager().convertToJavaString(buffer);
        }
        return cachedString;
    }

    /**
     * Return the length in bytes of the internal string
     * representation.
     *
     * @return length of internal representation
     */
    public int length() {
        return buffer.length;
    }

    /**
     * Return the internal byte array. The returned array should not
     * be modified, as it is used internally in
     * <code>DRDAString</code>. The value of the array might be
     * modified by subsequent calls to
     * <code>DRDAString.setBytes()</code>.
     *
     * @return internal buffer
     */
    public byte[] getBytes() {
        return buffer;
    }
}
