/*

   Derby - Class org.apache.derby.iapi.services.io.CompressedNumber

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.*;

/**
	Static methods to write and read compressed forms of numbers
	to DataOut and DataIn interfaces. Format written is platform
	independent like the Data* interfaces and must remain fixed
	once a product is shipped. If a different format is required
	then write a new set of methods, e.g. writeInt2. The formats
	defined by stored format identifiers are implicitly dependent
	on these formats not changing.
*/

public abstract class CompressedNumber {

	// the maximum number of bytes written out for an int
	public static final int MAX_INT_STORED_SIZE = 4;

	// the maximum number of bytes written out for a long
	public static final int MAX_LONG_STORED_SIZE = 8;

    // largest int stored compressed in 1 byte
	public static final int MAX_COMPRESSED_INT_ONE_BYTE  = 0x3f;

    // largest int stored compressed in 2 bytes
	public static final int MAX_COMPRESSED_INT_TWO_BYTES = 0x3fff;


	/**
		Write a compressed integer only supporting signed values.
		Formats are (with x representing value bits):
		<PRE>
		1 Byte - 00xxxxxx                              Represents the value <= 63 (0x3f)
		2 Byte - 01xxxxxx xxxxxxxx                     Represents the value > 63 && <= 16383 (0x3fff)
		4 byte - 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value > 16383 && <= MAX_INT
		</PRE>


		@exception IOException value is negative or an exception was thrown by a method on out.
	*/
	public static final int writeInt(DataOutput out, int value) throws IOException {

		if (value < 0)
			throw new IOException();

		if (value <= 0x3f) {

			out.writeByte(value);
			return 1;
		}

		if (value <= 0x3fff) {

			out.writeByte(0x40 | (value >>> 8));
			out.writeByte(value & 0xff);
			return 2;
		}

		out.writeByte(((value >>> 24) | 0x80) & 0xff);
		out.writeByte((value >>> 16) & 0xff);
		out.writeByte((value >>> 8) & 0xff);
		out.writeByte((value) & 0xff);
		return 4;
	}

	/**
		Write a compressed integer directly to an OutputStream.
		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int writeInt(OutputStream out, int value) throws IOException {

		if (value < 0)
			throw new IOException();

		if (value <= 0x3f) {

			out.write(value);
			return 1;
		}

		if (value <= 0x3fff) {

			out.write(0x40 | (value >>> 8));
			out.write(value & 0xff);
			return 2;
		}

		out.write(((value >>> 24) | 0x80) & 0xff);
		out.write((value >>> 16) & 0xff);
		out.write((value >>> 8) & 0xff);
		out.write((value) & 0xff);
		return 4;
	}


	/**
		Read an integer previously written by writeInt().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int readInt(DataInput in) throws IOException {

		int value = in.readUnsignedByte();

        if ((value & ~0x3f) == 0)
        {
            // length is stored in this byte, we also know that the 0x80 bit
            // was not set, so no need to mask off the sign extension from
            // the byte to int conversion.

            // account for 1 byte stored length of field + 1 for all returns
            return(value);
        }
		else if ((value & 0x80) == 0)
		{
            // length is stored in 2 bytes.  only use low 6 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x40) == 0x40);
            }

            // top 8 bits of 2 byte length is stored in this byte, we also 
            // know that the 0x80 bit was not set, so no need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the byte in data[offset + 1] to account for possible sign
            // extension.

            return(((value & 0x3f) << 8) | in.readUnsignedByte());
		}
        else
        {
            // length is stored in 4 bytes.  only use low 7 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x80) == 0x80);
            }

            // top 8 bits of 4 byte length is stored in this byte, we also 
            // know that the 0x80 bit was set, so need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the bytes from the next 3 bytes data[offset + 1,2,3] to 
            // account for possible sign extension.
            //
            return(
                 ((value & 0x7f)        << 24) |
                 (in.readUnsignedByte() << 16) |
                 (in.readUnsignedByte() <<  8) |
                 (in.readUnsignedByte()      ));
        }
	}

	/**
		Read an integer previously written by writeInt().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int readInt(InputStream in) throws IOException {

		int value = InputStreamUtil.readUnsignedByte(in);

        if ((value & ~0x3f) == 0)
        {
            return(value);
        }
		else if ((value & 0x80) == 0)
		{
            return(
                ((value & 0x3f) << 8) | InputStreamUtil.readUnsignedByte(in));
		}
        else
		{
            return(
                ((value          & 0x7f)              << 24) |
                (InputStreamUtil.readUnsignedByte(in) << 16) |
                (InputStreamUtil.readUnsignedByte(in) <<  8) |
                (InputStreamUtil.readUnsignedByte(in)      ));
		}
	}

	public static final int readInt(
    byte[]    data,
    int       offset) 
    {
		int value = data[offset++];

        if ((value & ~0x3f) == 0)
        {
            // length is stored in this byte, we also know that the 0x80 bit
            // was not set, so no need to mask off the sign extension from
            // the byte to int conversion.

            return(value);
        }
		else if ((value & 0x80) == 0)
		{
            // length is stored in 2 bytes.  only use low 6 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x40) == 0x40);
            }

            // top 8 bits of 2 byte length is stored in this byte, we also 
            // know that the 0x80 bit was not set, so no need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the byte in data[offset + 1] to account for possible sign
            // extension.

            return(((value & 0x3f) << 8) | (data[offset] & 0xff));
		}
        else
        {
            // length is stored in 4 bytes.  only use low 7 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x80) == 0x80);
            }

            // top 8 bits of 4 byte length is stored in this byte, we also 
            // know that the 0x80 bit was set, so need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the bytes from the next 3 bytes data[offset + 1,2,3] to 
            // account for possible sign extension.
            //
            return(
                ((value          & 0x7f) << 24) |
                ((data[offset++] & 0xff) << 16) |
                ((data[offset++] & 0xff) <<  8) |
                ((data[offset]   & 0xff)      ));
        }
	}

    /**
     * Return the compressed Int value + stored size of the length + 1
     * <p>
     * Given offset in array to beginning of compressed int, return the 
     * value of the compressed int + the number of bytes used to store the
     * length.
     * <p>
     * So 1 byte lengths will return: length + 1 + 1
     * So 2 byte lengths will return: length + 2 + 1
     * So 4 byte lengths will return: length + 4 + 1
     * <p>
     * Note that this routine will not work for lengths MAX_INT - (MAX_INT - 5).
     * <p>
     * This routine is currently used by the StorePage code to skip fields
     * as efficiently as possible.  Since the page size is less than 
     * (MAX_INT - 5) it is all right to use this routine.
     *
	 * @return compressed int value + length used to store the length.
     *
     * @param data   byte array containing the field.
     * @param offset offset to beginning of field, ie. data[offset] contains
     *               1st byte of the compressed int.
     **/
	public static final int readIntAndReturnIntPlusOverhead(
    byte[]  data,
    int     offset)
    {
        int value = data[offset];

        if ((value & ~0x3f) == 0)
        {
            // length is stored in this byte, we also know that the 0x80 bit
            // was not set, so no need to mask off the sign extension from
            // the byte to int conversion.

            // account for 1 byte stored length of field + 1 for all returns
            return(value + 2);
        }
		else if ((value & 0x80) == 0)
		{
            // length is stored in 2 bytes.  only use low 6 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x40) == 0x40);
            }

            // top 8 bits of 2 byte length is stored in this byte, we also 
            // know that the 0x80 bit was not set, so no need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the byte in data[offset + 1] to account for possible sign
            // extension.

            // add 3 to account for 2 byte length + 1 added to all returns

            return((((value & 0x3f) << 8) | (data[offset + 1] & 0xff)) + 3);
		}
        else
        {
            // length is stored in 4 bytes.  only use low 7 bits from 1st byte.

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT((value & 0x80) == 0x80);
            }

            // top 8 bits of 4 byte length is stored in this byte, we also 
            // know that the 0x80 bit was set, so need to mask off the 
            // sign extension from the 1st byte to int conversion.  Need to
            // mask the bytes from the next 3 bytes data[offset + 1,2,3] to 
            // account for possible sign extension.


            // add 5 to account for 4 byte length + 1 added to all returns
            return(
                (((value            & 0x7f) << 24) |
                 ((data[offset + 1] & 0xff) << 16) |
                 ((data[offset + 2] & 0xff) <<  8) |
                  (data[offset + 3] & 0xff)) + 5);
        }
	}


	/**
		Skip an integer previously written by writeInt().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int skipInt(DataInput in) throws IOException {

		int value = in.readUnsignedByte();

		if ((value & 0x80) == 0x80) {
			in.skipBytes(3);
			return 4;
		}

		if ((value & 0x40) == 0x40) {
			in.skipBytes(1);
			return 2;
		}

		return 1;
	}

	/**
		Skip an integer previously written by writeInt().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int skipInt(InputStream in) throws IOException {

		int value = InputStreamUtil.readUnsignedByte(in);

		int skipBytes = 0;

		if ((value & 0x80) == 0x80) {
			skipBytes = 3;
		}
		else if ((value & 0x40) == 0x40) {
			skipBytes = 1;
		}

		if (skipBytes != 0) {
			if (in.skip(skipBytes) != skipBytes)
				throw new EOFException();
		}


		return skipBytes + 1;
	}

	/**
		Return the number of bytes that would be written by a writeInt call
	*/
	public static final int sizeInt(int value) {
		if (value <= 0x3f) {
			return 1;
		}
		if (value <= 0x3fff) {
			return 2;
		}
		return 4;
	}

	/**
		Write a compressed long only supporting signed values.

		Formats are (with x representing value bits):
		<PRE>
		2 byte - 00xxxxxx xxxxxxxx                     Represents the value <= 16383 (0x3fff)
		4 byte - 01xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value > 16383  && <= 0x3fffffff
		8 byte - 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value > 0x3fffffff && <= MAX_LONG
		</PRE>


		@exception IOException value is negative or an exception was thrown by a method on out.
	*/
	public static final int writeLong(DataOutput out, long value) throws IOException {

		if (value < 0)
			throw new IOException();

		if (value <= 0x3fff) {

			out.writeByte((int) ((value >>> 8) & 0xff));
			out.writeByte((int) ((value      ) & 0xff));
			return 2;
		}

		if (value <= 0x3fffffff) {

			out.writeByte((int) (((value >>> 24) | 0x40) & 0xff));
			out.writeByte((int) ( (value >>> 16) & 0xff));
			out.writeByte((int) ( (value >>>  8) & 0xff));
			out.writeByte((int) ( (value       ) & 0xff));
			return 4;
		}

		out.writeByte((int) (((value >>> 56) | 0x80) & 0xff));
		out.writeByte((int) ( (value >>> 48) & 0xff));
		out.writeByte((int) ( (value >>> 40) & 0xff));
		out.writeByte((int) ( (value >>> 32) & 0xff));
		out.writeByte((int) ( (value >>> 24) & 0xff));
		out.writeByte((int) ( (value >>> 16) & 0xff));
		out.writeByte((int) ( (value >>>  8) & 0xff));
		out.writeByte((int) ( (value       ) & 0xff));
		return 8;
	}
	/**
		Write a compressed integer only supporting signed values.

		@exception IOException value is negative or an exception was thrown by a method on out.
	*/
	public static final int writeLong(OutputStream out, long value) throws IOException {

		if (value < 0)
			throw new IOException();

		if (value <= 0x3fff) {

			out.write((int) ((value >>> 8) & 0xff));
			out.write((int) ((value      ) & 0xff));
			return 2;
		}

		if (value <= 0x3fffffff) {

			out.write((int) (((value >>> 24) | 0x40) & 0xff));
			out.write((int) ( (value >>> 16) & 0xff));
			out.write((int) ( (value >>>  8) & 0xff));
			out.write((int) ( (value       ) & 0xff));
			return 4;
		}

		out.write((int) (((value >>> 56) | 0x80) & 0xff));
		out.write((int) ( (value >>> 48) & 0xff));
		out.write((int) ( (value >>> 40) & 0xff));
		out.write((int) ( (value >>> 32) & 0xff));
		out.write((int) ( (value >>> 24) & 0xff));
		out.write((int) ( (value >>> 16) & 0xff));
		out.write((int) ( (value >>>  8) & 0xff));
		out.write((int) ( (value       ) & 0xff));
		return 8;
	}

	/**
		Read a long previously written by writeLong().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final long readLong(DataInput in) throws IOException {

		int int_value = in.readUnsignedByte();

        if ((int_value & ~0x3f) == 0)
        {
            // test for small case first - assuming this is usual case.
            // this is stored in 2 bytes.

            return((int_value << 8) | in.readUnsignedByte());
		}
		else if ((int_value & 0x80) == 0)
		{
            // value is stored in 4 bytes.  only use low 6 bits from 1st byte.

            return(
                ((int_value & 0x3f)      << 24) |
                (in.readUnsignedByte()   << 16) |
                (in.readUnsignedByte()   <<  8) |
                (in.readUnsignedByte()));
		} 
        else
		{
            // value is stored in 8 bytes.  only use low 7 bits from 1st byte.
            return(
                (((long) (int_value & 0x7f)   ) << 56) |
                (((long) in.readUnsignedByte()) << 48) |
                (((long) in.readUnsignedByte()) << 40) |
                (((long) in.readUnsignedByte()) << 32) |
                (((long) in.readUnsignedByte()) << 24) |
                (((long) in.readUnsignedByte()) << 16) |
                (((long) in.readUnsignedByte()) <<  8) |
                (((long) in.readUnsignedByte())      ));
		}
	}

	/**
		Read a long previously written by writeLong().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final long readLong(InputStream in) throws IOException {

		int int_value = InputStreamUtil.readUnsignedByte(in);

        if ((int_value & ~0x3f) == 0)
        {
            // test for small case first - assuming this is usual case.
            // this is stored in 2 bytes.

            return((int_value << 8) | InputStreamUtil.readUnsignedByte(in));
		}
		else if ((int_value & 0x80) == 0)
		{
            // value is stored in 4 bytes.  only use low 6 bits from 1st byte.

            return(
                ((int_value      & 0x3f)              << 24) |
                (InputStreamUtil.readUnsignedByte(in) << 16) |
                (InputStreamUtil.readUnsignedByte(in) <<  8) |
                (InputStreamUtil.readUnsignedByte(in)      ));

		} 
        else
		{
            // value is stored in 8 bytes.  only use low 7 bits from 1st byte.
            long value = int_value;

            return(
                (((long) (value & 0x7f)                      ) << 56) |
                (((long) InputStreamUtil.readUnsignedByte(in)) << 48) |
                (((long) InputStreamUtil.readUnsignedByte(in)) << 40) |
                (((long) InputStreamUtil.readUnsignedByte(in)) << 32) |
                (((long) InputStreamUtil.readUnsignedByte(in)) << 24) |
                (((long) InputStreamUtil.readUnsignedByte(in)) << 16) |
                (((long) InputStreamUtil.readUnsignedByte(in)) <<  8) |
                (((long) InputStreamUtil.readUnsignedByte(in))      ));
		}
	}

	public static final long readLong(
    byte[]  data,
    int     offset)
    {
		int int_value = data[offset++];

        if ((int_value & ~0x3f) == 0)
        {
            // test for small case first - assuming this is usual case.
            // this is stored in 2 bytes.

            return((int_value << 8) | (data[offset] & 0xff));
		}
		else if ((int_value & 0x80) == 0)
		{
            // value is stored in 4 bytes.  only use low 6 bits from 1st byte.

            return(
                ((int_value      & 0x3f) << 24) |
                ((data[offset++] & 0xff) << 16) |
                ((data[offset++] & 0xff) <<  8) |
                ((data[offset]   & 0xff)      ));

		} 
        else
		{
            // value is stored in 8 bytes.  only use low 6 bits from 1st byte.
            return(
                (((long) (int_value      & 0x7f)) << 56) |
                (((long) (data[offset++] & 0xff)) << 48) |
                (((long) (data[offset++] & 0xff)) << 40) |
                (((long) (data[offset++] & 0xff)) << 32) |
                (((long) (data[offset++] & 0xff)) << 24) |
                (((long) (data[offset++] & 0xff)) << 16) |
                (((long) (data[offset++] & 0xff)) <<  8) |
                (((long) (data[offset]   & 0xff))      ));

		}
	}

	/**
		Skip a long previously written by writeLong().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int skipLong(DataInput in) throws IOException {

		long value = in.readUnsignedByte();

		if ((value & 0x80) == 0x80)
		{
			in.skipBytes(7);
			return 8;
		}
		
		
		if ((value & 0x40) == 0x40)
		{
			in.skipBytes(3);
			return 4;

		}

		in.skipBytes(1);
		return 2;
	}

	/**
		Skip a long previously written by writeLong().

		@exception IOException an exception was thrown by a method on in.
	*/
	public static final int skipLong(InputStream in) throws IOException {

		int value = InputStreamUtil.readUnsignedByte(in);

		int skipBytes;

		if ((value & 0x80) == 0x80) {
			skipBytes = 7;
		}
		else if ((value & 0x40) == 0x40) {
			skipBytes = 3;
		}
		else
			skipBytes = 1;

		if (in.skip(skipBytes) != skipBytes)
			throw new EOFException();

		return skipBytes + 1;
	}

	public static final int sizeLong(long value) {

		if (value <= 0x3fff) {

			return 2;
		}

		if (value <= 0x3fffffff) {
			return 4;
		}

		return 8;
	}

// /* FOR TESTING
// *****************************************************

  	private static byte[] holder = new byte[8];
	private static  ArrayOutputStream aos = new ArrayOutputStream(holder);
	private static  DataOutput out = new DataOutputStream(aos);

	private static  ArrayInputStream ais = new ArrayInputStream(holder);
	private static  DataInput in = new DataInputStream(ais);
	private static  InputStream in_stream = ais;


    private static short checkInt(int i, short oldLength) throws IOException {

		aos.setPosition(0);
		int length = CompressedNumber.writeInt(out, i);
		if (length != oldLength) {
			System.out.println("changing length to " + length + " at value " + i + " 0x" + Integer.toHexString(i));

			oldLength = (short) length;
		}

		int writtenBytes = aos.getPosition();
		if (writtenBytes != length) {
			System.out.println("MISMATCH written bytes  expected " + length + " got " + writtenBytes);
			System.exit(1);
		}

		if (length != CompressedNumber.sizeInt(i)) {
			System.out.println("MISMATCH sizeInt() bytes  expected " + length + " got " + CompressedNumber.sizeInt(i));
			System.exit(1);
		}

		ais.setPosition(0);
		int value = CompressedNumber.readInt(in);
		if (value != i) {
			System.out.println("MISMATCH value readInt(DataInput) expected " + i + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		value = ais.readCompressedInt();
		if (value != i) {
			System.out.println("MISMATCH value readInt(DataInput) expected " + i + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		value = CompressedNumber.readInt(in_stream);
		if (value != i) {
			System.out.println("MISMATCH value in readInt(InputStream) expected " + i + " got " + value);
			System.exit(1);
		}


		value = CompressedNumber.readInt(holder, 0);
		if (value != i) {
			System.out.println(
                "MISMATCH frome readInt(byte[], offset) value expected " + 
                i + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		int skipLength = CompressedNumber.skipInt(in);
		if (skipLength != length) {
			System.out.println("MISMATCH skip length expected " + length + " got " + skipLength);
			System.exit(1);
		}

        int value_plus_int_length = readIntAndReturnIntPlusOverhead(holder, 0);
        if (value_plus_int_length != (length + i + 1)) { 
			System.out.println("MISMATCH readIntAndReturnIntPlusOverhead() return expected " + (length + i) + " got " + value_plus_int_length);
			System.exit(1);
        }

		int skipPosition = ais.getPosition();
		if (skipPosition != length) {
			System.out.println("MISMATCH skip position expected " + length + " got " + skipPosition);
			System.exit(1);
		}

		return oldLength;
	}

    private static short checkLong(long i, short oldLength) throws IOException {

		aos.setPosition(0);
		int length = CompressedNumber.writeLong(out, i);
		if (length != oldLength) {
			System.out.println("changing length to " + length + " at value " + i + " 0x" + Long.toHexString(i));
			oldLength = (short) length;
		}

		int writtenBytes = aos.getPosition();
		if (writtenBytes != length) {
			System.out.println("MISMATCH written bytes  expected " + length + " got " + writtenBytes);
			System.exit(1);
		}

		if (length != CompressedNumber.sizeLong(i)) {
			System.out.println("MISMATCH sizeLong() bytes  expected " + length + " got " + CompressedNumber.sizeLong(i));
			System.exit(1);
		}

		long value = CompressedNumber.readLong(holder, 0);
		if (value != i) {
			for (int j = 0; j < 8; j++) {

				System.out.println(Integer.toHexString((int) holder[j]));
			}

			System.out.println(
                "MISMATCH in readLong(byte[], offset) value expected " + 
                Long.toHexString(i) + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		value = CompressedNumber.readLong(in_stream);
		if (value != i) {
			for (int j = 0; j < 8; j++) {

				System.out.println(Integer.toHexString((int) holder[j]));
			}
			System.out.println("MISMATCH value in readLong(InputStream) expected " + Long.toHexString(i) + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		value = ais.readCompressedLong();
		if (value != i) {
			for (int j = 0; j < 8; j++) {

				System.out.println(Integer.toHexString((int) holder[j]));
			}
			System.out.println("MISMATCH value in readLong(InputStream) expected " + Long.toHexString(i) + " got " + value);
			System.exit(1);
		}


		ais.setPosition(0);
		value = CompressedNumber.readLong(in);
		if (value != i) {
			for (int j = 0; j < 8; j++) {

				System.out.println(Integer.toHexString((int) holder[j]));
			}
			System.out.println("MISMATCH value in readLong(DataInput) expected " + Long.toHexString(i) + " got " + value);
			System.exit(1);
		}

		ais.setPosition(0);
		int skipLength = CompressedNumber.skipLong(in);
		if (skipLength != length) {
			System.out.println("MISMATCH skip length expected " + length + " got " + skipLength);
			System.exit(1);
		}

		int skipPosition = ais.getPosition();
		if (skipPosition != length) {
			System.out.println("MISMATCH skip position expected " + length + " got " + skipPosition);
			System.exit(1);
		}

		return oldLength;
	}

	public static void main(String[] args) throws IOException {

		short oldLength = -1;

		System.out.println("** Testing Int");

		oldLength = checkInt(0, oldLength);
		oldLength = checkInt(1, oldLength);
		oldLength = checkInt(2, oldLength);

		oldLength = checkInt(0x3f - 4, oldLength);
		oldLength = checkInt(0x3f - 3, oldLength);
		oldLength = checkInt(0x3f - 2, oldLength);
		oldLength = checkInt(0x3f - 1, oldLength);
		oldLength = checkInt(0x3f    , oldLength);
		oldLength = checkInt(0x3f + 1, oldLength);
		oldLength = checkInt(0x3f + 2, oldLength);
		oldLength = checkInt(0x3f + 3, oldLength);
		oldLength = checkInt(0x3f + 4, oldLength);

		oldLength = checkInt(0x3f80 - 4, oldLength);
		oldLength = checkInt(0x3f80 - 3, oldLength);
		oldLength = checkInt(0x3f80 - 2, oldLength);
		oldLength = checkInt(0x3f80 - 1, oldLength);
		oldLength = checkInt(0x3f80    , oldLength);
		oldLength = checkInt(0x3f80 + 1, oldLength);
		oldLength = checkInt(0x3f80 + 2, oldLength);
		oldLength = checkInt(0x3f80 + 3, oldLength);
		oldLength = checkInt(0x3f80 + 4, oldLength);

		oldLength = checkInt(0x3fff - 4, oldLength);
		oldLength = checkInt(0x3fff - 3, oldLength);
		oldLength = checkInt(0x3fff - 2, oldLength);
		oldLength = checkInt(0x3fff - 1, oldLength);
		oldLength = checkInt(0x3fff    , oldLength);
		oldLength = checkInt(0x3fff + 1, oldLength);
		oldLength = checkInt(0x3fff + 2, oldLength);
		oldLength = checkInt(0x3fff + 3, oldLength);
		oldLength = checkInt(0x3fff + 4, oldLength);

		oldLength = checkInt(Integer.MAX_VALUE - 4, oldLength);
		oldLength = checkInt(Integer.MAX_VALUE - 3, oldLength);
		oldLength = checkInt(Integer.MAX_VALUE - 2, oldLength);
		oldLength = checkInt(Integer.MAX_VALUE - 1, oldLength);
		oldLength = checkInt(Integer.MAX_VALUE    , oldLength);

        oldLength = -1;
        for (int i = 0; i < 0xf0000; i++)
        {
            oldLength = checkInt(i, oldLength);
        }

        // takes 30 minutes to run.
        //
        // for (int i = 0; i < Integer.MAX_VALUE; i++)
        // {
        // if (i % 0x00800000 == 0)
        // System.out.println("checking: " + i);
        //
        // oldLength = checkInt(i, oldLength);
        // }


		System.out.println("** Testing Long");

        oldLength = -1;
        for (int i = 0; i < 0xf0000; i++)
        {
            oldLength = checkLong(i, oldLength);
        }
	
		oldLength = -1;
		
		oldLength = checkLong(0, oldLength);
		oldLength = checkLong(1, oldLength);
		oldLength = checkLong(2, oldLength);

		oldLength = checkLong(0x3fff - 2, oldLength);
		oldLength = checkLong(0x3fff - 1, oldLength);
		oldLength = checkLong(0x3fff    , oldLength);
		oldLength = checkLong(0x3fff + 1, oldLength);
		oldLength = checkLong(0x3fff + 2, oldLength);

		oldLength = checkLong(0x3fffffff - 4, oldLength);
		oldLength = checkLong(0x3fffffff - 3, oldLength);
		oldLength = checkLong(0x3fffffff - 2, oldLength);
		oldLength = checkLong(0x3fffffff - 1, oldLength);
		oldLength = checkLong(0x3fffffff    , oldLength);
		oldLength = checkLong(0x3fffffff + 1, oldLength);
		oldLength = checkLong(0x3fffffff + 2, oldLength);
		oldLength = checkLong(0x3fffffff + 3, oldLength);
		oldLength = checkLong(0x3fffffff + 4, oldLength);

		oldLength = checkLong(0x70000000 - 2, oldLength);
		oldLength = checkLong(0x70000000 - 1, oldLength);
		oldLength = checkLong(0x70000000    , oldLength);
		oldLength = checkLong(0x70000000 + 1, oldLength);
		oldLength = checkLong(0x70000000 + 2, oldLength);


		oldLength = checkLong(Long.MAX_VALUE - 2, oldLength);
		oldLength = checkLong(Long.MAX_VALUE - 1, oldLength);
		oldLength = checkLong(Long.MAX_VALUE    , oldLength);


	}
// ********************************************************/
}
