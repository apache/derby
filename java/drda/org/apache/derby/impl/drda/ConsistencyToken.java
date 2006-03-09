/*
 * Derby - class org.apache.derby.impl.drda.ConsistencyToken
 *
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
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
        if (hash == 0) {
            int len = bytes.length;
            for (int i = 0; i < len; ++i) {
                hash ^= bytes[i];
            }
        }
        return hash;
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
