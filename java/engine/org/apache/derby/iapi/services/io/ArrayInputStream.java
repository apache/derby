/*

   Derby - Class org.apache.derby.iapi.services.io.ArrayInputStream

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

import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.EOFException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.LimitObjectInput;
import org.apache.derby.iapi.services.io.ErrorObjectInput;

import java.io.UTFDataFormatException;

/**
	An InputStream that allows reading from an array of bytes. The array
	of bytes that is read from can be changed without having to create a new
	instance of this class.
*/
public final class ArrayInputStream extends InputStream implements LimitObjectInput {

	private byte[] pageData;

	private int		start;
	private int		end;		// exclusive
	private int		position;

	public ArrayInputStream() {
		this(null);
	}

	private ErrorObjectInput oi;

	public ArrayInputStream(byte[] data) {
		super();
		setData(data);
		oi = new org.apache.derby.iapi.services.io.FormatIdInputStream(this);
	}

	public ArrayInputStream(byte[] data, int offset, int length) throws IOException {
		this(data);
		setLimit(offset, length);
	}

	/*
	** Public methods
	*/

	/**
		Set the array of bytes to be read.
	*/
	public void setData(byte[] data) {
		pageData = data;
		clearLimit();
	}

	public void setData(byte[] data, int offset, int length) throws IOException {
		pageData = data;
		setLimit(offset, length);
	}

	/**
		Return a reference to the array of bytes this stream is going to read
		from so that caller may load it with stuff 
	*/
	public byte[] getData()
	{
		return pageData;
	}

	/*
	** Methods of InputStream
	*/

	public int read() throws IOException {
		if (position == end)
			return -1; // end of file

		return pageData[position++] & 0xff ;

	}

	public int read(byte b[], int off, int len) throws IOException {

		if ((position + len) > end) {

			len = end - position;

			if (len == 0) {
				return -1; // end of file
			}
		}

		System.arraycopy(pageData, position, b, off, len);
		position += len;
		return len;
	}

	public long skip(long count)  throws IOException {

		if ((position + count) > end) {

			count = end - position;

			if (count == 0)
				return 0; // end of file
		}

		position += count;

		return count;

	}

	public int getPosition() {
		return position;
	}

	public final void setPosition(int newPosition)
		throws IOException {

        if ((newPosition >= start) && (newPosition < end))
            position = newPosition;
        else
			throw new EOFException();
	}

	public int available() throws IOException {

		return end - position;
	}


	/**
		A setLimit which also sets the position to be offset.

		@exception IOException limit is out of range 
	*/
	public int setLimit(int offset, int length) throws IOException {

		if ((offset < 0) || (length < 0)) {
			start = end = position = 0;
			throw new EOFException();
		}

		start = offset;
		end = offset + length;

		if (end > pageData.length) {
			start = end = position = 0;
			throw new EOFException();
		}

		position = start;

		return length;
	}

	/*
	** Methods of Limit
	*/

	public final void setLimit(int length) throws IOException {

        start = position;
        end   = position + length;

        if (end <= pageData.length)
        {
            return;
        }
        else
        {
			start = end = position = 0;
			throw new EOFException();
        }
	}

	/**
		Clears the limit by setting the limit to be the entire byte array.

		@see Limit#clearLimit
	*/
	public final int clearLimit() {

		if (pageData != null) {
			start = 0;
			int remainingBytes = end - position;
			end = pageData.length;
			return remainingBytes;
		} else {
			start = end = position = 0;
			return 0;
		}
	}

	/*
	** Methods of DataInput
	*/

    public final void readFully(byte b[]) throws IOException {
		readFully(b, 0, b.length);
    }

    public final void readFully(byte b[], int off, int len) throws IOException {

		if ((position + len) > end) {

			throw new EOFException();
		}

		System.arraycopy(pageData, position, b, off, len);
		position += len;
	}

    public final int skipBytes(int n) throws IOException {
		if ((position + n) > end) {

			throw new EOFException();
		}
		position += n;
		return n;
    }

    public final boolean readBoolean() throws IOException {
		if (position == end)
			throw new EOFException(); // end of file

		return pageData[position++] != 0;
    }

    public final byte readByte() throws IOException {
		if (position == end)
			throw new EOFException(); // end of file

		return pageData[position++];
    }

    public final int readUnsignedByte() throws IOException {
		if (position == end)
			throw new EOFException(); // end of file

		return pageData[position++] & 0xff ;
    }

    public final short readShort() throws IOException {

		int pos = position;
		byte[] data = pageData;

		if (pos >= (end - 1))
			throw new EOFException(); // end of file

		int s = ((data[pos++] & 0xff) << 8) | (data[pos++] & 0xff);

		position = pos;

		return (short) s;
    }

   public final int readUnsignedShort() throws IOException {
 		int    pos  = position;
		byte[] data = pageData;

		if (pos >= (end - 1))
			throw new EOFException(); // end of file

		int us = ((data[pos++] & 0xff) << 8) | (data[pos++] & 0xff);

		position = pos;

		return us;
   }

    public final char readChar() throws IOException {
 		int    pos  = position;
		byte[] data = pageData;

		if (pos >= (end -1))
			throw new EOFException(); // end of file

		int c = ((data[pos++] & 0xff) << 8) | (data[pos++] & 0xff);

		position = pos;

		return (char) c;
    }

    public final int readInt() throws IOException {

 		int pos = position;
		byte[] data = pageData;

		if (pos >= (end - 3))
			throw new EOFException(); // end of file



		int i = ((data[pos++] & 0xff) << 24) |
                ((data[pos++] & 0xff) << 16) |
                ((data[pos++] & 0xff) <<  8) |
                ((data[pos++] & 0xff)      );

		position = pos;

		return i;
    }

    public final long readLong() throws IOException {
 		int    pos  = position;
		byte[] data = pageData;

		if (pos >= (end - 7))
			throw new EOFException(); // end of file

		long l = 
            (((long) (data[pos++] & 0xff)) << 56) |
            (((long) (data[pos++] & 0xff)) << 48) |
            (((long) (data[pos++] & 0xff)) << 40) |
            (((long) (data[pos++] & 0xff)) << 32) |
            (((long) (data[pos++] & 0xff)) << 24) |
            (((long) (data[pos++] & 0xff)) << 16) |
            (((long) (data[pos++] & 0xff)) <<  8) | 
            (((long) (data[pos++] & 0xff))      );

		position = pos;

		return l;
    }

    public final float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
    }

    public final String readLine() throws IOException {
		return oi.readLine();
    }
    public final String readUTF() throws IOException {
		return oi.readUTF();
    }

    /**
     * read in a cloudscape UTF formated string into a char[].
     * <p>
     * This routine inline's the code to read a UTF format string from a
     * byte[] array (pageData), into a char[] array.  The string will
     * be read into the char[] array passed into this routine through
     * rawData_array[0] if it is big enough.  If it is not big enough
     * a new char[] will be alocated and returned to the caller by putting
     * it into rawData_array[0].
     * <p>
     * To see detailed description of the cloudscape UTF format see
     * the writeExternal() routine of SQLChar.
     * <p>
     * The routine returns the number of char's read into the returned
     * char[], note that this length may smaller than the actual length
     * of the char[] array.
     *
	 * @return The the number of valid char's in the returned char[].
     *
     * @param rawData_array This parameter uses a element array to implement
     *                      an in/out function parameter.  The char[] array
     *                      in rawData_array[0] is used to read the data into
     *                      unless it is not big enough, then a new array
     *                      is allocated and the old one discarded.  In 
     *                      either case on return rawData_array[0] contains
     *                      the filled in char[] - caller must allow that
     *                      the array may or may not be different from the
     *                      one passed in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public final int readCloudscapeUTF(char[][] rawData_array) 
        throws IOException
	{
        // copy globals locally, to give compiler chance to optimize.
        byte[]  data    = pageData;
        int     end_pos = end;
 		int     pos     = position;

        // get header length - stored as an unsigned short.

		int utflen;
        if (pos + 1 < end_pos) 
        {
            utflen = (((data[pos++] & 0xff) << 8) | (data[pos++] & 0xff));
        }
        else
        {
			throw new EOFException(); // end of file
        }

        /**
         * 3 cases - can they all happen?
         *
         * o utflen == 0 and end is marked E0, 0, 0
         * o utflen == 0 and there is no data (ie. 0 length string)
         * o utflen != 0, utflen is exact length of following bytes
         **/

        // requiredLength is the amount of bytes to read from the array,
        // either the utflen in the header length, or the number of bytes
        // available in the array.  Throw an exception if we know up front
        // that utflen is bigger than number of bytes in the array.
		int requiredLength;
        if (utflen != 0)
        {
            // this is the only place we need to check for end of file, 
            // the subsequent loop will not read past bytes_available_in_array.

            if (utflen <= (end_pos - pos))
            {
                requiredLength = utflen;
            }
            else
            {
                throw new EOFException();
            }
        }
        else
        {
            // the byte header returned 0, so read what is left in the array.
            
            requiredLength = (end_pos - pos);
        }

        // Use the passed in char[] array if it is long enough, otherwise
        // allocate a new array, and will pass it back to caller at the end.
        // Note that requiredLength is the worst case length for the array,
        // as the number of char characters must be <= number of bytes (ie.
        // all characters were stored compressed in 1 byte each - the ascii
        // default) - if there are any 2 or 3 byte stored characters then
        // the array will have extra space at the end.  "strlen" tracks the
        // real number of char's in str[].
        char[] str = rawData_array[0];
		if ((str == null) || (requiredLength > str.length)) 
        {
			str = new char[requiredLength];
            rawData_array[0] = str;
		} 

        end_pos = pos + requiredLength;
        int strlen = 0;

        while (pos < end_pos)
        {
			int char1 = (data[pos++] & 0xff);

			// top fours bits of the first unsigned byte that maps to a 1,2 
            // or 3 byte character
			//
			// 0000xxxx	- 0 - 1 byte char
			// 0001xxxx - 1 - 1 byte char
			// 0010xxxx - 2 - 1 byte char
			// 0011xxxx - 3 - 1 byte char
			// 0100xxxx - 4 - 1 byte char
			// 0101xxxx - 5 - 1 byte char
			// 0110xxxx - 6 - 1 byte char
			// 0111xxxx - 7 - 1 byte char
			// 1000xxxx - 8 - error
			// 1001xxxx - 9 - error
			// 1010xxxx - 10 - error
			// 1011xxxx - 11 - error
			// 1100xxxx - 12 - 2 byte char
			// 1101xxxx - 13 - 2 byte char
			// 1110xxxx - 14 - 3 byte char
			// 1111xxxx - 15 - error

			int char2, char3;
			if ((char1 & 0x80) == 0x00)
			{
				// one byte character
				str[strlen++] = (char) char1;
			}
			else if ((char1 & 0x60) == 0x40) // we know the top bit is set here
			{ 
				// two byte character, make sure read of next byte is in bounds.
                if (pos >= end_pos)
					throw new UTFDataFormatException();		  

                char2 = (data[pos++] & 0xff);

				if ((char2 & 0xC0) != 0x80)
					throw new UTFDataFormatException();		  

				str[strlen++] = (char)(((char1 & 0x1F) << 6) | (char2 & 0x3F));
			}
			else if ((char1 & 0x70) == 0x60) // we know the top bit is set here
			{
				// three byte character

				// 3 byte character, make sure read of next 2 bytes in bounds.
                if (pos + 1 >= end_pos)
					throw new UTFDataFormatException();		  

                char2 = (data[pos++] & 0xff);
                char3 = (data[pos++] & 0xff);

				if ((char1 == 0xE0) && 
                    (char2 ==    0) && 
                    (char3 ==    0) && 
                    (utflen == 0))
				{
					// we reached the end of a long string,
					// that was terminated with
					// (11100000, 00000000, 00000000)
                    break;
				}
                else if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                {
					throw new UTFDataFormatException();		  
                }
                else
                {
                    str[strlen++] = (char)
                        (((char1 & 0x0F) << 12) | 
                         ((char2 & 0x3F) <<  6) | 
                         ((char3 & 0x3F) <<  0));
                }
			}
			else 
            {
				throw new UTFDataFormatException();
			}

		}

        // update global on successful read exit.
        position = pos;

        return(strlen);
	}

    /**
     * Read a compressed int from the stream.
     * <p>
     * Read a compressed int from the stream, which is assumed to have
     * been written by a call to CompressNumber.writeInt().
     * <p>
     * Code from CompressedNumber is inlined here so that these fields can
     * be read from the array with a minimum of function calls.
     * <p>
     * The format of a compressed int is as follows:
     *
     * Formats are (with x representing value bits):
     * <PRE>
     * 1 Byte- 00xxxxxx                            val <= 63 (0x3f)
     * 2 Byte- 01xxxxxx xxxxxxxx                   val > 63 && <= 16383 (0x3fff)
     * 4 byte- 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx val > 16383 && <= MAX_INT
     * </PRE>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public final int readCompressedInt()
        throws IOException
    {
 		int    pos  = position;
		byte[] data = pageData;

        try
        {
            int value = data[pos++];

            if ((value & ~0x3f) == 0)
            {
                // entire value is stored in this byte, we also know that the 
                // 0x80 bit was not set, so no need to mask off the sign 
                // extension from the byte to int conversion.
            }
            else if ((value & 0x80) == 0)
            {
                // value stored in 2 bytes.  only use low 6 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x40) == 0x40);
                }

                // top 8 bits of 2 byte value is stored in this byte, we also 
                // know that the 0x80 bit was not set, so no need to mask off 
                // the sign extension from the 1st byte to int conversion.  
                // Need to mask the byte in data[pos + 1] to account for 
                // possible sign extension.

                value = 
                    (((value & 0x3f) << 8) | (data[pos++] & 0xff));
            }
            else
            {
                // value stored in 4 bytes.  only use low 7 bits from 1st byte.

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT((value & 0x80) == 0x80);
                }

                // top 8 bits of 4 byte value is stored in this byte, we also 
                // know that the 0x80 bit was set, so need to mask off the 
                // sign extension from the 1st byte to int conversion.  Need to
                // mask the bytes from the next 3 bytes data[pos + 1,2,3] to 
                // account for possible sign extension.
                //

                value = 
                    ((value       & 0x7f) << 24) |
                    ((data[pos++] & 0xff) << 16) |
                    ((data[pos++] & 0xff) <<  8) |
                    ((data[pos++] & 0xff)      );
            }

            position = pos;

            return(value);
        }
        catch (java.lang.ArrayIndexOutOfBoundsException ex)
        {
			throw new EOFException(); // end of file
        }

    }

    /**
     * Read a compressed long from the stream.
     * <p>
     * Read a compressed long from the stream, which is assumed to have
     * been written by a call to CompressNumber.writeLong().
     * <p>
     * Code from CompressedNumber is inlined here so that these fields can
     * be read from the array with a minimum of function calls.
     * <p>
     * The format of a compressed int is as follows:
     *
     * Formats are (with x representing value bits):
     * <PRE>
     * value <= 16383 (0x3fff): 
     *     2 byte - 00xxxxxx xxxxxxxx 
     *
     * value > 16383 && <= 0x3fffffff:
     *     4 byte - 01xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
     *
     * value > 0x3fffffff && <= MAX_LONG:
     *     8 byte - 1xxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
     * </PRE>
     *
     **/
    public final long readCompressedLong()
        throws IOException
    {
        try
        {
            // copy globals locally, to give compiler chance to optimize.
            int     pos         = position;
            byte[]  data        = pageData;

            // int_value tells whether it is 1, 4, or 8 bytes long.
            int     int_value   = data[pos++];

            // build up long value and return it through this variable.
            long    long_value;

            if ((int_value & ~0x3f) == 0)
            {
                // 2 byte representation

                // 1st byte of value is stored in int_value, we also know that 
                // the 0x80 bit was not set, so no need to mask off the sign 
                // extension from the 1st byte to int conversion.
                long_value = ((int_value << 8) | (data[pos++] & 0xff));
            }
            else if ((int_value & 0x80) == 0)
            {
                // value stored in 4 bytes.  only use low 6 bits from 1st byte.

                // Need to mask the bytes from the next 3 bytes 
                // data[pos + 1,2,3] to account for possible sign extension.

                long_value = 
                    ((int_value   & 0x3f) << 24) |
                    ((data[pos++] & 0xff) << 16) |
                    ((data[pos++] & 0xff) <<  8) |
                    ((data[pos++] & 0xff)      );
            }
            else
            {
                // top 7 bits of 4 byte value is stored in int_value, we also 
                // know that the 0x80 bit was set, so need to mask off the 
                // sign extension from the 1st byte to int conversion.  Need to
                // mask the bytes from the next 7 bytes data[pos + 1,2,...] to 
                // account for possible sign extension.
                //

                // value stored in 8 bytes.  only use low 6 bits from 1st byte.
                long_value = 
                    (((long) (int_value   & 0x7f)) << 56) |
                    (((long) (data[pos++] & 0xff)) << 48) |
                    (((long) (data[pos++] & 0xff)) << 40) |
                    (((long) (data[pos++] & 0xff)) << 32) |
                    (((long) (data[pos++] & 0xff)) << 24) |
                    (((long) (data[pos++] & 0xff)) << 16) |
                    (((long) (data[pos++] & 0xff)) <<  8) |
                    (((long) (data[pos++] & 0xff))      );
            }

            position = pos;

            return(long_value);
        }
        catch (java.lang.ArrayIndexOutOfBoundsException ex)
        {
            // let java figure out if we went past end of data[] array.
            
			throw new EOFException(); // end of file
        }
    }

	public Object readObject() throws ClassNotFoundException, IOException {
		return oi.readObject();
	}

	public String getErrorInfo()  {
		return oi.getErrorInfo();
	}

    public Exception getNestedException() {
        return oi.getNestedException();
    }
}
