/*

   Derby - Class org.apache.derby.iapi.services.io.InputStreamUtil

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

import java.io.*;

/**
	Utility methods for InputStream that are stand-ins for
	a small subset of DataInput methods. This avoids pushing
	a DataInputStream just to get this functionality.
*/
public final class InputStreamUtil {

	/**
		Read an unsigned byte from an InputStream, throwing an EOFException
		if the end of the input is reached.

		@exception IOException if an I/O error occurs.
		@exception EOFException if the end of the stream is reached

		@see DataInput#readUnsignedByte
	
	*/
	public static int readUnsignedByte(InputStream in) throws IOException {
		int b = in.read();
		if (b < 0)
			throw new EOFException();

		return b;
	}

	/**
		Read a number of bytes into an array.

		@exception IOException if an I/O error occurs.
		@exception EOFException if the end of the stream is reached

		@see DataInput#readFully

	*/
	public static void readFully(InputStream in, byte b[],
                                 int offset,
                                 int len) throws IOException
	{
		do {
			int bytesRead = in.read(b, offset, len);
			if (bytesRead < 0)
				throw new EOFException();
			len -= bytesRead;
			offset += bytesRead;
		} while (len != 0);
	}


	/**
		Read a number of bytes into an array.
        Keep reading in a loop until len bytes are read or EOF is reached or
        an exception is thrown. Return the number of bytes read.
        (InputStream.read(byte[],int,int) does not guarantee to read len bytes
         even if it can do so without reaching EOF or raising an exception.)

		@exception IOException if an I/O error occurs.
	*/
	public static int readLoop(InputStream in,
                                byte b[],
                                int offset,
                                int len)
        throws IOException
	{
        int firstOffset = offset;
		do {
			int bytesRead = in.read(b, offset, len);
			if (bytesRead <= 0)
                break;
			len -= bytesRead;
			offset += bytesRead;
		} while (len != 0);
        return offset - firstOffset;
	}


	/**
		Skip a number of bytes in the stream. Note that this version takes and returns
		a long instead of the int used by skipBytes.

		@exception IOException if an I/O error occurs.
		@exception EOFException if the end of the stream is reached

		@see DataInput#skipBytes
	*/
	public static long skipBytes(InputStream in, long n) throws IOException {

		while (n > 0) {
			//System.out.println(" skip n = " + n);
			long delta = in.skip(n);
			//System.out.println(" skipped = " + delta);
			if (delta < 0)
				throw new EOFException();
			n -= delta;
		}

		return n;
	}
}
