/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.InputStream;
import java.io.IOException;

/**
	An InputStream that is like java.io.ByteArrayInputStream but supports
	a close() call that causes all methods to throw an IOException.
	Java's ByteInputStream has a close() method that does not do anything.
*/
public final class NewByteArrayInputStream extends InputStream {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private byte[] data;
	private int offset;
	private int length;

	public NewByteArrayInputStream(byte[] data) {
		this(data, 0, data.length);
	}

	public NewByteArrayInputStream(byte[] data, int offset, int length) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

	/*
	** Public methods
	*/
	public int read() throws IOException {
		if (data == null)
			throw new IOException();

		if (length == 0)
			return -1; // end of file

		length--;

		return data[offset++] & 0xff ;

	}

	public int read(byte b[], int off, int len) throws IOException {
		if (data == null)
			throw new IOException();

		if (length == 0)
			return -1;

		if (len > length)
			len = length;

		System.arraycopy(data, offset, b, off, len);
		offset += len;
		length -= len;
		return len;
	}

	public long skip(long count)  throws IOException {
		if (data == null)
			throw new IOException();

		if (length == 0)
			return -1;

		if (count > length)
			count = length;

		offset += (int) count;
		length -= (int) count;

		return count;

	}

	public int available() throws IOException
	{
		if (data == null)
			throw new IOException();

		return length;
	}

    public	byte[]	getData() { return data; }

	public void close()
	{
		data = null;
	}

}
