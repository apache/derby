/*

   Derby - Class org.apache.derby.iapi.services.io.NewByteArrayInputStream

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

/**
	An InputStream that is like java.io.ByteArrayInputStream but supports
	a close() call that causes all methods to throw an IOException.
	Java's ByteInputStream has a close() method that does not do anything.
*/
public final class NewByteArrayInputStream extends InputStream {

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
