/*

   Derby - Class org.apache.derby.iapi.services.cache.SignedBinary

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derby.impl.drda;

/**
 * Converters from signed binary bytes to Java <code>short</code>, <code>int</code>, or <code>long</code>.
 */
public class SignedBinary
{
  // Hide the default constructor, this is a static class.
  private SignedBinary () {}

  /**
   * AS/400, Unix, System/390 byte-order for signed binary representations.
   */
  public final static int BIG_ENDIAN = 1;

  /**
   * Intel 80/86 reversed byte-order for signed binary representations.
   */
  public final static int LITTLE_ENDIAN = 2;

  /**
   * Build a Java short from a 2-byte signed binary representation.
   * <p>
   * Depending on machine type, byte orders are
   * {@link #BIG_ENDIAN BIG_ENDIAN} for signed binary integers, and
   * {@link #LITTLE_ENDIAN LITTLE_ENDIAN} for pc8087 signed binary integers.
   *
   * @exception IllegalArgumentException if the specified byte order is not recognized.
   */
  public static short getShort (byte[] buffer, int offset, int byteOrder)
  {
    switch (byteOrder) {
    case BIG_ENDIAN:
      return bigEndianBytesToShort (buffer, offset);
    case LITTLE_ENDIAN:
      return littleEndianBytesToShort (buffer, offset);
    default:
      throw new java.lang.IllegalArgumentException();
    }
  }

  /**
   * Build a Java int from a 4-byte signed binary representation.
   * <p>
   * Depending on machine type, byte orders are
   * {@link #BIG_ENDIAN BIG_ENDIAN} for signed binary integers, and
   * {@link #LITTLE_ENDIAN LITTLE_ENDIAN} for pc8087 signed binary integers.
   *
   * @exception IllegalArgumentException if the specified byte order is not recognized.
   */
  public static int getInt (byte[] buffer, int offset, int byteOrder)
  {
    switch (byteOrder) {
    case BIG_ENDIAN:
      return bigEndianBytesToInt (buffer, offset);
    case LITTLE_ENDIAN:
      return littleEndianBytesToInt (buffer, offset);
    default:
      throw new java.lang.IllegalArgumentException();
    }
  }

  /**
   * Build a Java long from an 8-byte signed binary representation.
   * <p>
   * Depending on machine type, byte orders are
   * {@link #BIG_ENDIAN BIG_ENDIAN} for signed binary integers, and
   * {@link #LITTLE_ENDIAN LITTLE_ENDIAN} for pc8087 signed binary integers.
   * <p>
   *
   * @exception IllegalArgumentException if the specified byte order is not recognized.
   */
  public static long getLong (byte[] buffer, int offset, int byteOrder)
  {
    switch (byteOrder) {
    case BIG_ENDIAN:
      return bigEndianBytesToLong (buffer, offset);
    case LITTLE_ENDIAN:
      return littleEndianBytesToLong (buffer, offset);
    default:
      throw new java.lang.IllegalArgumentException();
    }
  }

  /**
   * Build a Java short from a 2-byte big endian signed binary representation.
   */
  public static short bigEndianBytesToShort (byte[] buffer, int offset)
  {
    return (short) (((buffer[offset+0] & 0xff) << 8) +
                    ((buffer[offset+1] & 0xff) << 0));
  }

  /**
   * Build a Java short from a 2-byte little endian signed binary representation.
   */
  public static short littleEndianBytesToShort (byte[] buffer, int offset)
  {
    return (short) (((buffer[offset+0] & 0xff) << 0) +
                    ((buffer[offset+1] & 0xff) << 8));
  }

  /**
   * Build a Java int from a 4-byte big endian signed binary representation.
   */
  public static int bigEndianBytesToInt (byte[] buffer, int offset)
  {
    return (int) (((buffer[offset+0] & 0xff) << 24) +
                  ((buffer[offset+1] & 0xff) << 16) +
                  ((buffer[offset+2] & 0xff) << 8) +
                  ((buffer[offset+3] & 0xff) << 0));
  }

  /**
   * Build a Java int from a 4-byte little endian signed binary representation.
   */
  public static int littleEndianBytesToInt (byte[] buffer, int offset)
  {
    return (int) (((buffer[offset+0] & 0xff) << 0) +
                  ((buffer[offset+1] & 0xff) << 8) +
                  ((buffer[offset+2] & 0xff) << 16) +
                  ((buffer[offset+3] & 0xff) << 24));
  }

  /**
   * Build a Java long from an 8-byte big endian signed binary representation.
   */
  public static long bigEndianBytesToLong (byte[] buffer, int offset)
  {
    return (long) (((buffer[offset+0] & 0xffL) << 56) +
                   ((buffer[offset+1] & 0xffL) << 48) +
                   ((buffer[offset+2] & 0xffL) << 40) +
                   ((buffer[offset+3] & 0xffL) << 32) +
                   ((buffer[offset+4] & 0xffL) << 24) +
                   ((buffer[offset+5] & 0xffL) << 16) +
                   ((buffer[offset+6] & 0xffL) << 8) +
                   ((buffer[offset+7] & 0xffL) << 0));
  }

  /**
   * Build a Java long from an 8-byte little endian signed binary representation.
   */
  public static long littleEndianBytesToLong (byte[] buffer, int offset)
  {
    return (long) (((buffer[offset+0] & 0xffL) << 0) +
                   ((buffer[offset+1] & 0xffL) << 8) +
                   ((buffer[offset+2] & 0xffL) << 16) +
                   ((buffer[offset+3] & 0xffL) << 24) +
                   ((buffer[offset+4] & 0xffL) << 32) +
                   ((buffer[offset+5] & 0xffL) << 40) +
                   ((buffer[offset+6] & 0xffL) << 48) +
                   ((buffer[offset+7] & 0xffL) << 56));
  }
}
