/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
