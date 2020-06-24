/*
 * Derby - class org.apache.derby.impl.drda.ConsistencyToken
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
 * Class which represents an RDB Package Consistency Token.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-212
final class ConsistencyToken {
    /** Byte array representation of the token. */
    private final byte[] bytes;
    /** Cached hash code. */
    private int hash = 0;

    /**
     * Create a new <code>ConsistencyToken</code> instance.
     *
     * @param bytes byte array representing the token
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
    ConsistencyToken(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Get the byte array representation of the consistency token.
     *
     * @return a <code>byte[]</code> value
     */
    public byte[] getBytes() {
        return bytes;
    }

    /**
     * Check whether this object is equal to another object.
     *
     * @param o another object
     * @return true if the objects are equal
     */
    public boolean equals(Object o) {
        if (!(o instanceof ConsistencyToken)) return false;
        ConsistencyToken ct = (ConsistencyToken) o;
        int len = bytes.length;
        if (len != ct.bytes.length) return false;
        for (int i = 0; i < len; ++i) {
            if (bytes[i] != ct.bytes[i]) return false;
        }
        return true;
    }

    /**
     * Calculate the hash code.
     *
     * @return hash code
     */
    public int hashCode() {
        // ConsistencyToken objects might be kept for a long time and are
        // frequently used as keys in hash tables. Therefore, it is a good idea
        // to cache their hash codes.
        int h = hash;
//IC see: https://issues.apache.org/jira/browse/DERBY-1688
        if (h == 0) {
            // The hash code has not been calculated yet (or perhaps the hash
            // code actually is 0). Calculate a new one and cache it. No
            // synchronization is needed since reads and writes of 32-bit
            // primitive values are guaranteed to be atomic. See The
            // "Double-Checked Locking is Broken" Declaration for details.
            int len = bytes.length;
            for (int i = 0; i < len; ++i) {
                h ^= bytes[i];
            }
            hash = h;
        }
        return h;
    }

    /**
     * Return a string representation of the consistency token by
     * converting it to a <code>BigInteger</code> value. (For
     * debugging only.)
     *
     * @return a <code>String</code> value
     */
    public String toString() {
        return new java.math.BigInteger(bytes).toString();
    }
}
