/*

   Derby - Class org.apache.derby.iapi.types.RawToBinaryFormatStream

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

package org.apache.derby.iapi.types;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

/**
	Stream that takes a raw input stream and converts it
	to the on-disk format of the binary types by prepending the
	length of the value.
    <P>
    If the length of the stream is known then it is encoded
    as the first bytes in the stream in the defined format.
    <BR>
    If the length is unknown then the first four bytes will
    be zero, indicating unknown length.
    <BR>
    Note: This stream cannot be re-used. Once end of file is
    reached, the next read call will throw an EOFException
    
    @see SQLBinary
*/
public final class RawToBinaryFormatStream extends LimitInputStream {

    /**
     * Number of bytes of length encoding.
     * 
     */
	private int encodedOffset;
    
    /**
     * Encoding of the length in bytes which will be
     * seen as the first encodedLength.length bytes of
     * this stream.
     */
    private byte[] encodedLength;
    
    // flag to indicate the stream has already been read
    // and eof reached
    private boolean eof = false;

    /**
     * The length of the stream.
     * Unknown if less than 0.
     */
    private final int length;
    /**
     * The maximum allowed length for the stream.
     * No limit if less than 0.
     */
    private final int maximumLength;
    /**
     * The type of the column the stream is inserted into.
     * Used for length less streams, <code>null</code> if not in use.
     */
    private final String typeName;

    /**
     * Create a binary on-disk stream from the given <code>InputStream</code>.
     *
     * The on-disk stream prepends a length encoding, and validates that the
     * actual length of the stream matches the specified length (as according
     * to JDBC 3.0).
     *
     * @param in application's raw binary stream passed into JDBC layer
     * @param length length of the stream
     * @throws IllegalArgumentException if <code>length</code> is negative.
     *      This exception should never be exposed to the user, and seeing it
     *      means a programming error exists in the code.
     */
    public RawToBinaryFormatStream(InputStream in, int length) {
        super(in);
        if (length < 0) {
            throw new IllegalArgumentException(
                    "Stream length cannot be negative: " + length);
        }
        this.length = length;
        this.maximumLength = -1;
        this.typeName = null;

        setLimit(length);
        
        if (length <= 31)
        {
            encodedLength = new byte[1];               
            encodedLength[0] = (byte) (0x80 | (length & 0xff));
        }
        else if (length <= 0xFFFF)
        {
            encodedLength = new byte[3];
            encodedLength[0] = (byte) 0xA0;
            encodedLength[1] = (byte)(length >> 8);
            encodedLength[2] = (byte)(length);    
        }
        else
        {
            encodedLength = new byte[5];
            encodedLength[0] = (byte) 0xC0;
            encodedLength[1] = (byte)(length >> 24);
            encodedLength[2] = (byte)(length >> 16);
            encodedLength[3] = (byte)(length >> 8);
            encodedLength[4] = (byte)(length);
        }
    }

    /**
     * Create a binary on-disk stream from the given <code>InputStream</code>
     * of unknown length.
     *
     * A limit is placed on the maximum length of the stream.
     *
     * @param in the application stream
     * @param maximumLength maximum length of the column data is inserted into
     * @param typeName type name for the column data is inserted into
     * @throws IllegalArgumentException if maximum length is negative, or type
     *      name is <code>null</code>. This exception should never be exposed
     *      to the user, and seeing it means a programming error exists in the
     *      code. Although a missing type name is not critical, an exception is
     *      is thrown to signal the intended use of this constructor.
     */
    public RawToBinaryFormatStream(InputStream in,
                                   int maximumLength,
                                   String typeName) {
        super(in);
        if (maximumLength < 0) {
            throw new IllegalArgumentException("Maximum length for a capped " +
                    "stream cannot be negative: " + maximumLength);
        }
        if (typeName == null) {
            throw new IllegalArgumentException("Type name cannot be null");
        }
        this.length = -1;
        this.maximumLength = maximumLength;
        this.typeName = typeName;
        // Unknown length, four zero bytes.
        encodedLength = new byte[4];
        setLimit(maximumLength);
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
        
		if (encodedOffset < encodedLength.length) {
            return encodedLength[encodedOffset++] & 0xff;
 		}

		int ret = super.read();

		if (ret == -1)
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

        if (length > -1 && remainingBytes > 0) {
            throw new DerbyIOException(
                        MessageService.getTextMessage(
                                    SQLState.SET_STREAM_INEXACT_LENGTH_DATA),
                        SQLState.SET_STREAM_INEXACT_LENGTH_DATA);
        }

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
			if (c != -1) {
                if (length > -1) {
                    // Stream is not capped, and should have matched the
                    // specified length.
                    throw new DerbyIOException(
                                MessageService.getTextMessage(
                                    SQLState.SET_STREAM_INEXACT_LENGTH_DATA),
                                SQLState.SET_STREAM_INEXACT_LENGTH_DATA);
                } else {
                    // Stream is capped, and has exceeded the maximum length.
                    throw new DerbyIOException(
                            MessageService.getTextMessage(
                                    SQLState.LANG_STRING_TRUNCATION,
                                    typeName,
                                    "XXXX",
                                    String.valueOf(maximumLength)),
                            SQLState.LANG_STRING_TRUNCATION);
                }
            }
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

		int elen = encodedLength.length - encodedOffset;

		if (elen != 0) {
			if (len < elen)
				elen = len;
            System.arraycopy(encodedLength, encodedOffset,
                    b, off, elen);

            encodedOffset += elen;

			off += elen;
			len -= elen;
            
            if (len == 0)
                return elen;
		}

		int realRead = super.read(b, off, len);

		if (realRead < 0)
		{
			if (elen != 0)
				return elen;

			checkSufficientData();
			return realRead;
		}

		return elen + realRead;
	}
}
