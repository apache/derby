/*

   Derby - Class org.apache.derby.client.am.FloatingPoint

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package org.apache.derby.client.am;

/**
 * Converters from floating point bytes to Java <code>float</code>, <code>double</code>, or
 * <code>java.math.BigDecimal</code>.
 */
public class FloatingPoint {
    // Hide the default constructor, this is a static class.
    private FloatingPoint() {
    }

    /**
     * Supported Unix Big Endian IEEE 754 floating point representation.
     */
    public final static int IEEE_754_FLOATING_POINT = 0x48;

    //--------------------------private helper methods----------------------------

    /**
     * Convert the byte array to an int.
     */
    private static final int convertFromByteToInt(byte[] buffer, int offset) {
        return (buffer[offset] << 24) |
                ((buffer[offset + 1] & 0xFF) << 16) |
                ((buffer[offset + 2] & 0xFF) << 8) |
                (buffer[offset + 3] & 0xFF);
    }

    /**
     * Convert the byte array to a long.
     */
    private static final long convertFromByteToLong(byte[] buffer, int offset) {
        return ((buffer[offset] & 0xFFL) << 56) |
                ((buffer[offset + 1] & 0xFFL) << 48) |
                ((buffer[offset + 2] & 0xFFL) << 40) |
                ((buffer[offset + 3] & 0xFFL) << 32) |
                ((buffer[offset + 4] & 0xFFL) << 24) |
                ((buffer[offset + 5] & 0xFFL) << 16) |
                ((buffer[offset + 6] & 0xFFL) << 8) |
                (buffer[offset + 7] & 0xFFL);
    }


    //--------------entry points for runtime representation-----------------------

    /**
     * Build a Java float from a 4-byte floating point representation.
     * <p/>
     * This includes DERBY types: <ul> <li> REAL <li> FLOAT(1<=n<=24) </ul>
     *
     * @throws IllegalArgumentException if the specified representation is not recognized.
     */
    public static final float getFloat(byte[] buffer, int offset) {
        return Float.intBitsToFloat(convertFromByteToInt(buffer, offset));
    }

    /**
     * Build a Java double from an 8-byte floating point representation.
     * <p/>
     * <p/>
     * This includes DERBY types: <ul> <li> FLOAT <li> DOUBLE [PRECISION] </ul>
     *
     * @throws IllegalArgumentException if the specified representation is not recognized.
     */
    public static final double getDouble(byte[] buffer, int offset) {
        return Double.longBitsToDouble(convertFromByteToLong(buffer, offset));
    }

    //--------------entry points for runtime representation-----------------------

    /**
     * Write a Java <code>float</code> to a 4-byte floating point representation.
     */
    public static final void floatToIeee754Bytes(byte[] buffer, int offset, float f) {
        int intBits = Float.floatToIntBits(f);
        buffer[offset] = (byte) ((intBits >>> 24) & 0xFF);
        buffer[offset + 1] = (byte) ((intBits >>> 16) & 0xFF);
        buffer[offset + 2] = (byte) ((intBits >>> 8) & 0xFF);
        buffer[offset + 3] = (byte) (intBits & 0xFF);
    }

    /**
     * Write a Java <code>double</code> to an 8-byte double precision floating point representation.
     */
    public static final void doubleToIeee754Bytes(byte[] buffer, int offset, double d) {
        long longBits = Double.doubleToLongBits(d);
        buffer[offset] = (byte) ((longBits >>> 56) & 0xFF);
        buffer[offset + 1] = (byte) ((longBits >>> 48) & 0xFF);
        buffer[offset + 2] = (byte) ((longBits >>> 40) & 0xFF);
        buffer[offset + 3] = (byte) ((longBits >>> 32) & 0xFF);
        buffer[offset + 4] = (byte) ((longBits >>> 24) & 0xFF);
        buffer[offset + 5] = (byte) ((longBits >>> 16) & 0xFF);
        buffer[offset + 6] = (byte) ((longBits >>> 8) & 0xFF);
        buffer[offset + 7] = (byte) (longBits & 0xFF);
    }
}
