/*

   Derby - Class org.apache.derby.iapi.types.RawToBinaryFormatStream

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

package org.apache.derby.iapi.types;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
	Stream that takes a raw input stream and converts it
	to the on-disk format of the binary types by prepending the
	length of the value. In this case 0 is always written.
    Note: This stream cannot be re-used. Once end of file is
    reached, the next read call will throw an EOFException
*/
public class RawToBinaryFormatStream extends LimitInputStream {

	private int dummyBytes = 4;
    
    // flag to indicate the stream has already been read
    // and eof reached
    private boolean eof = false;

	/**
		@param	in Application's raw binary stream passed into JDBC layer
		@param	length - length of the stream, if known, otherwise -1.
	*/
	public RawToBinaryFormatStream(InputStream in, int length) {
		super(in);
		if (length >= 0) {
			setLimit(length);
		}
	}

	/**
		Read from the wrapped stream prepending the intial bytes if needed.
        If stream has been read, and eof reached, in that case any subsequent
        read will throw an EOFException
	*/
	public int read() throws IOException {

        if ( eof )
            throw new EOFException(MessageService.getTextMessage
                        (SQLState.STREAM_EOF));
        
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
        // if we reached here, then read call returned -1, and we 
        // have already reached the end of stream, so set eof=true
        // so that subsequent reads on this stream will return an 
        // EOFException
        eof = true;
		if (!limitInPlace)
        return;

		int remainingBytes = clearLimit();

		if (remainingBytes > 0)
			throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INEXACT_LENGTH_DATA));

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
				throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INEXACT_LENGTH_DATA));
		}
	}

	/**
		Read from the wrapped stream prepending the intial bytes if needed.
        If stream has been read, and eof reached, in that case any subsequent
        read will throw an EOFException
	*/
	public int read(byte b[], int off, int len) throws IOException {
  
        if ( eof )
            throw new EOFException(MessageService.getTextMessage(SQLState.STREAM_EOF));

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
