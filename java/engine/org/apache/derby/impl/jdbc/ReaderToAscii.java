/*

   Derby - Class org.apache.derby.impl.jdbc.ReaderToAscii

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.sql.SQLException;

/**
	ReaderToAscii converts Reader (with characters) to a stream of ASCII characters.
*/
public final class ReaderToAscii extends InputStream
{

	private final Reader data;
	private char[]	conv;
	private boolean closed;

	public ReaderToAscii(Reader data) 
	{
		this.data = data;
		if (!(data instanceof UTF8Reader))
			conv = new char[256];
	}

	public int read() throws IOException
	{
		if (closed)
			throw new IOException();

		int c = data.read();
		if (c == -1)
			return -1;

		if (c <= 255)
			return c & 0xFF;
		else
			return '?'; // Question mark - out of range character.
	}

	public int read(byte[] buf, int off, int len) throws IOException
	{
		if (closed)
			throw new IOException();

		if (data instanceof UTF8Reader) {

			return ((UTF8Reader) data).readAsciiInto(buf, off, len);
		}

		if (len > conv.length)
			len = conv.length;

		len = data.read(conv, 0, len);
		if (len == -1)
			return -1;

		for (int i = 0; i < len; i++) {
			char c = conv[i];

			byte cb;
			if (c <= 255)
				cb = (byte) c;
			else
				cb = (byte) '?'; // Question mark - out of range character.
				
			buf[off++] = cb;
		}

		return len;
	}

	public long skip(long len) throws IOException {
		if (closed)
			throw new IOException();

		return data.skip(len);
	}

	public void close() throws IOException
	{
		if (!closed) {
			closed = true;
			data.close();
		}
	}
}
