/*

   Derby - Class org.apache.derby.iapi.types.ReaderToUTF8Stream

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
import java.io.Reader;
import java.io.UTFDataFormatException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derby.iapi.services.io.LimitReader;
import org.apache.derby.iapi.types.TypeId;

/**
	Converts a java.io.Reader to the on-disk UTF8 format used by Derby
    for character types.
*/
public final class ReaderToUTF8Stream
	extends InputStream
{
    /**
     * Application's reader wrapped in a LimitReader.
     */
	private LimitReader reader;

	private byte[] buffer;
	private int boff;
	private int blen;
	private boolean eof;
	private boolean multipleBuffer;
    // buffer to hold the data read from stream 
    // and converted to UTF8 format
    private final static int BUFSIZE = 32768;
    
    /** Number of characters to truncate from this stream
     The SQL standard allows for truncation of trailing spaces 
     for clobs,varchar,char.
     If zero, no characters are truncated.
     */
    private final int charsToTruncate;
    private static final char SPACE = ' ';
    
    /**
     * Length of the final value, after truncation if any,
     * in characters.
     this stream needs to fit into a column of colWidth
     if truncation error happens ,then the error message includes 
     information about the column width.
    */
    private final int valueLength; 
    /** The maximum allowed length of the stream. */
    private final int maximumLength;
    /** The type name for the column data is inserted into. */
    private final String typeName;
    
    /**
     * Create a stream that will truncate trailing blanks if required/allowed.
     *
     * If the stream must be truncated, the number of blanks to truncate
     * is specified to allow the stream to be checked for exact length, as
     * required by JDBC 3.0. If the stream is shorter or longer than specified,
     * an exception is thrown during read.
     *
     * @param appReader application reader
     * @param valueLength the length of the reader in characters
     * @param numCharsToTruncate the number of trailing blanks to truncate
     * @param typeName type name of the column data is inserted into
     */
    public ReaderToUTF8Stream(Reader appReader,
                              int valueLength,
                              int numCharsToTruncate,
                              String typeName) {
        this.reader = new LimitReader(appReader);
        reader.setLimit(valueLength);
        buffer = new byte[BUFSIZE];
        blen = -1;        
        this.charsToTruncate = numCharsToTruncate;
        this.valueLength = valueLength;
        this.maximumLength = -1;
        this.typeName = typeName;
    }

    /**
     * Create a UTF-8 stream for a length less application reader.
     *
     * A limit is placed on the length of the reader. If the reader exceeds
     * the maximum length, truncation of trailing blanks is attempted. If
     * truncation fails, an exception is thrown.
     *
     * @param appReader application reader
     * @param maximumLength maximum allowed length in number of characters for
     *      the reader
     * @param typeName type name of the column data is inserted into
     * @throws IllegalArgumentException if maximum length is negative, or type
     *      name is <code>null<code>
     */
    public ReaderToUTF8Stream(Reader appReader,
                              int maximumLength,
                              String typeName) {
        if (maximumLength < 0) {
            throw new IllegalArgumentException("Maximum length for a capped " +
                    "stream cannot be negative: " + maximumLength);
        }
        if (typeName == null) {
            throw new IllegalArgumentException("Type name cannot be null");
        }
        this.reader = new LimitReader(appReader);
        reader.setLimit(maximumLength);
        buffer = new byte[BUFSIZE];
        blen = -1;
        this.maximumLength = maximumLength;
        this.typeName = typeName;
        this.charsToTruncate = -1;
        this.valueLength = -1;
    }

    /**
     * read from stream; characters converted to utf-8 derby specific encoding.
     * If stream has been read, and eof reached, in that case any subsequent
     * read will throw an EOFException
     * @see java.io.InputStream#read()
     */
	public int read() throws IOException {

        // when stream has been read and eof reached, stream is closed
        // and buffer is set to null ( see close() method)
        // since stream cannot be re-used, check if stream is closed and 
        // if so throw an EOFException
        if ( buffer == null)
            throw new EOFException(MessageService.getTextMessage(SQLState.STREAM_EOF));

        
		// first read
		if (blen < 0)
			fillBuffer(2);

		while (boff == blen)
		{
			// reached end of buffer, read more?
			if (eof)
            {
               // we have reached the end of this stream
               // cleanup here and return -1 indicating 
               // eof of stream
               close();
               return -1;
            }
                

			fillBuffer(0);
		}

		return buffer[boff++] & 0xff;

	}

	public int read(byte b[], int off, int len) throws IOException {
        
        // when stream has been read and eof reached, stream is closed
        // and buffer is set to null ( see close() method)
        // since stream cannot be re-used, check if stream is closed and 
        // if so throw an EOFException
        if ( buffer == null )
            throw new EOFException(MessageService.getTextMessage
                    (SQLState.STREAM_EOF));

        // first read
		if (blen < 0)
			fillBuffer(2);

		int readCount = 0;

		while (len > 0)
		{

			int copyBytes = blen - boff;

			// buffer empty?
			if (copyBytes == 0)
			{
				if (eof)
				{
                    if (readCount > 0)
                    {
                        return readCount;
                    }
                    else
                    {
                        // we have reached the eof, so close the stream
                        close();
                        return -1;  
                    }
                    
				}
				fillBuffer(0);
				continue;
			}

			if (len < copyBytes)
				copyBytes = len;

			System.arraycopy(buffer, boff, b, off, copyBytes);
			boff += copyBytes;
			len -= copyBytes;
			readCount += copyBytes;
            off += copyBytes;

		}
		return readCount;
	}

	private void fillBuffer(int startingOffset) throws IOException
	{
		int off = boff = startingOffset;

		if (off == 0)
			multipleBuffer = true;

		// 6! need to leave room for a three byte UTF8 encoding
		// and 3 bytes for our special end of file marker.
		for (; off <= buffer.length - 6; )
		{
			int c = reader.read();
			if (c < 0) {
				eof = true;
				break;
			}

			if ((c >= 0x0001) && (c <= 0x007F))
            {
				buffer[off++] = (byte) c;
			}
            else if (c > 0x07FF)
            {
				buffer[off++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				buffer[off++] = (byte) (0x80 | ((c >>  6) & 0x3F));
				buffer[off++] = (byte) (0x80 | ((c >>  0) & 0x3F));
			}
            else
            {
				buffer[off++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
				buffer[off++] = (byte) (0x80 | ((c >>  0) & 0x3F));
			}
		}

		blen = off;
		boff = 0;

		if (eof)
			checkSufficientData();
	}

    /**
     * Validate the length of the stream, take corrective action if allowed.
     *
     * JDBC 3.0 (from tutorial book) requires that an input stream has the
     * correct number of bytes in the stream.
     * If the stream is too long, trailing blank truncation is attempted if
     * allowed. If truncation fails, or is disallowed, an exception is thrown.
     *
     * @throws IOException if an errors occurs in the application stream
     * @throws DerbyIOException if Derby finds a problem with the stream;
     *      stream is too long and cannot be truncated, or the stream length
     *      does not match the specified length
     */
	private void checkSufficientData() throws IOException
	{
		// now that we finished reading from the stream; the amount
        // of data that we can insert,start check for trailing spaces
        if (charsToTruncate > 0)
        {
            reader.setLimit(charsToTruncate);
            truncate();
        }
        
        // A length less stream that is capped, will return 0 even if there
        // are more bytes in the application stream.
        int remainingBytes = reader.clearLimit();
        if (remainingBytes > 0 && valueLength > 0) {
            // If we had a specified length, throw exception.
            throw new DerbyIOException(
                    MessageService.getTextMessage(
                        SQLState.SET_STREAM_INEXACT_LENGTH_DATA),
                    SQLState.SET_STREAM_INEXACT_LENGTH_DATA);
        }

		// if we had a limit try reading one more character.
		// JDBC 3.0 states the stream must have the correct number of
        // characters in it.
        if (remainingBytes == 0 && reader.read() >= 0) {
            if (valueLength > -1) {
                throw new DerbyIOException(
                        MessageService.getTextMessage(
                            SQLState.SET_STREAM_INEXACT_LENGTH_DATA),
                        SQLState.SET_STREAM_INEXACT_LENGTH_DATA);
            } else {
                // Stream was capped (length less) and too long.
                // Try to truncate if allowed, or else throw exception.
                if (canTruncate()) {
                    truncate();
                } else {
                    throw new DerbyIOException(
                            MessageService.getTextMessage(
                                SQLState.LANG_STRING_TRUNCATION),
                            SQLState.LANG_STRING_TRUNCATION);
                }
            }
        }
		
		// can put the correct length into the stream.
		if (!multipleBuffer)
		{
			int utflen = blen - 2;

			buffer[0] = (byte) ((utflen >>> 8) & 0xFF);
			buffer[1] = (byte) ((utflen >>> 0) & 0xFF);

		}
		else
		{
			buffer[blen++] = (byte) 0xE0;
			buffer[blen++] = (byte) 0x00;
			buffer[blen++] = (byte) 0x00;
		}
	}

    /**
     * Determine if trailing blank truncation is allowed.
     */
    private boolean canTruncate() {
        // Only a few types can be truncated, default is to not allow.
        if (typeName.equals(TypeId.CLOB_NAME)) {
            return true;
        } else if (typeName.equals(TypeId.VARCHAR_NAME)) {
            return true;
        }
        return false;
    }

    /**
     * Attempt to truncate the stream by removing trailing blanks.
     */
    private void truncate()
            throws IOException {
        int c = 0;
        for (;;) {
            c = reader.read();

            if (c < 0) {
                break;
            } else if (c != SPACE) {
                throw new DerbyIOException(
                    MessageService.getTextMessage(
                        SQLState.LANG_STRING_TRUNCATION,
                        typeName, 
                        "XXXX", 
                        String.valueOf(valueLength)),
                    SQLState.LANG_STRING_TRUNCATION);
            }
        }
    }

    /**
     * return resources 
     */
	public void close() throws IOException
	{
        // since stream has been read and eof reached, return buffer back to 
        // the vm.
        // Instead of using another variable to indicate stream is closed
        // a check on (buffer==null) is used instead. 
        buffer = null;
	}

    /**
     * Return an optimized version of bytes available to read from 
     * the stream 
     * Note, it is not exactly per java.io.InputStream#available()
     */
    public final int available()
    {
       int remainingBytes = reader.getLimit();
       // this object buffers BUFSIZE bytes that can be read 
       // and when that is finished it reads the next available bytes
       // from the reader object 
       // reader.getLimit() returns the remaining bytes available
       // on this stream
       return (BUFSIZE > remainingBytes ? remainingBytes : BUFSIZE);
    }
  }

