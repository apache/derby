/*

   Derby - Class org.apache.derby.iapi.services.io.CompressedNumber

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.shared.common.sanity.SanityManager;

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
		1 Byte - 00xxxxxx                              Represents the value &lt;= 63 (0x3f)
		2 Byte - 01xxxxxx xxxxxxxx                     Represents the value &gt; 63 &amp;&amp; &lt;= 16383 (0x3fff)
		4 byte - 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value &gt; 16383 &amp;&amp; &lt;= MAX_INT
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
		Skip an integer previously written by writeInt().

		@exception IOException an exception was thrown by a method on in.
	*/

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
		2 byte - 00xxxxxx xxxxxxxx                     Represents the value &lt;= 16383 (0x3fff)
		4 byte - 01xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value &gt; 16383  &amp;&amp; &lt;= 0x3fffffff
		8 byte - 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx   Represents the value &gt; 0x3fffffff &amp;&amp; &lt;= MAX_LONG
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

	public static final int sizeLong(long value) {

		if (value <= 0x3fff) {

			return 2;
		}

		if (value <= 0x3fffffff) {
			return 4;
		}

		return 8;
	}
}
