/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.jdbc;

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;

import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
	Stream that takes a raw input stream and converts it
	to the format of the binary types by prepending the
	length of the value. In this case 0 is always written.
*/
class RawToBinaryFormatStream extends LimitInputStream {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

	private int dummyBytes = 4;

	/**
		@param	in Application's raw binary stream passed into JDBC layer
		@param	length - length of the stream, if known, otherwise -1.
	*/
	RawToBinaryFormatStream(InputStream in, int length) {
		super(in);
		if (length >= 0) {
			setLimit(length);
		}
	}

	/**
		Read from the wrapped stream prepending the intial bytes if needed.
	*/
	public int read() throws IOException {

		if (dummyBytes != 0) {
			dummyBytes--;
			return 0;
		}

		int ret = super.read();

		if (ret < 0)
			checkSufficientData();

		return ret;
	}

	/**
		JDBC 3.0 (from tutorial book) requires that an
		input stream has the correct number of bytes in
		the stream.
	*/
	private void checkSufficientData() throws IOException
	{
		if (!limitInPlace)
			return;

		int remainingBytes = clearLimit();

		if (remainingBytes > 0)
			throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INSUFFICIENT_DATA));

		// if we had a limit try reading one more byte.
		// JDBC 3.0 states the stream muct have the correct number of characters in it.
		if (remainingBytes == 0) {
			int c;
			try
			{
				c = super.read();
			}
			catch (IOException ioe) {
				c = -1;
			}
			if (c >= 0)
				throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INSUFFICIENT_DATA));
		}
	}

	/**
		Read from the wrapped stream prepending the intial bytes if needed.
	*/
	public int read(byte b[], int off, int len) throws IOException {

		int dlen = dummyBytes;

		if (dlen != 0) {
			if (len < dlen)
				dlen = len;
			for (int i = 0; i < dlen; i++) {
				b[off+i] = 0;
			}
			dummyBytes -= dlen;

			off += dlen;
			len -= dlen;
		}

		int realRead = super.read(b, off, len);

		if (realRead < 0)
		{
			if (dlen != 0)
				return dlen;

			checkSufficientData();

			return realRead;
		}

		return dlen + realRead;
	}
}
