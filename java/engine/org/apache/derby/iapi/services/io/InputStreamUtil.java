/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
