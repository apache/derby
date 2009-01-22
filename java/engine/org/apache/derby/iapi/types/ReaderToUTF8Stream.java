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
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.derby.iapi.services.io.LimitReader;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Converts the characters served by a {@code java.io.Reader} to a stream
 * returning the data in the on-disk modified UTF-8 encoded representation used
 * by Derby.
 * <p>
 * Length validation is performed. If required and allowed by the target column
 * type, truncation of blanks will also be performed.
 */
public final class ReaderToUTF8Stream
	extends InputStream
{
    /**
     * Application's reader wrapped in a LimitReader.
     */
	private LimitReader reader;

    /** Constant indicating the first iteration of {@code fillBuffer}. */
    private final static int FIRST_READ = Integer.MIN_VALUE;
    /**
     * Size of buffer to hold the data read from stream and converted to the
     * modified UTF-8 format.
     */
    private final static int BUFSIZE = 32768;
    private byte[] buffer = new byte[BUFSIZE];
	private int boff;
    private int blen = -1;
	private boolean eof;
    /** Tells if the stream content is/was larger than the buffer size. */
	private boolean multipleBuffer;
    /**
     * The generator for the stream header to use for this stream.
     * @see #checkSufficientData()
     */
    private final StreamHeaderGenerator hdrGen;
    /** The length of the header. */
    private int headerLength;

    /**
     * Number of characters to truncate from this stream.
     * The SQL standard allows for truncation of trailing spaces for CLOB,
     * VARCHAR and CHAR. If zero, no characters are truncated, unless the
     * stream length execeeds the maximum length of the column we are inserting
     * into.
     */
    private final int charsToTruncate;
    private static final char SPACE = ' ';
    
    /**
     * If positive, length of the expected final value, after truncation if any,
     * in characters. If negative, the maximum length allowed in the column we
     * are inserting into. A negative value means we are working with a stream
     * of unknown length, inserted through one of the JDBC 4.0 "lengthless
     * override" methods.
     */
    private final int valueLength; 
    /** The type name for the column data is inserted into. */
    private final String typeName;
    /** The number of chars encoded. */
    private int charCount;
    
    /**
     * Create a stream that will truncate trailing blanks if required/allowed.
     *
     * If the stream must be truncated, the number of blanks to truncate
     * is specified to allow the stream to be checked for exact length, as
     * required by JDBC 3.0. If the stream is shorter or longer than specified,
     * an exception is thrown during read.
     *
     * @param appReader application reader
     * @param valueLength the expected length of the reader in characters
     *      (positive), or the inverse (maxColWidth * -1) of the maximum column
     *      width if the expected stream length is unknown
     * @param numCharsToTruncate the number of trailing blanks to truncate
     * @param typeName type name of the column data is inserted into
     * @param headerGenerator the stream header generator
     */
    public ReaderToUTF8Stream(Reader appReader,
                              int valueLength,
                              int numCharsToTruncate,
                              String typeName,
                              StreamHeaderGenerator headerGenerator) {
        this.reader = new LimitReader(appReader);
        reader.setLimit(valueLength);
        this.charsToTruncate = numCharsToTruncate;
        this.valueLength = valueLength;
        this.typeName = typeName;
        this.hdrGen = headerGenerator;
        if (SanityManager.DEBUG) {
            // Check the type name
            // The national types (i.e. NVARCHAR) are not used/supported.
            SanityManager.ASSERT(typeName != null && (
                    typeName.equals(TypeId.CHAR_NAME) ||
                    typeName.equals(TypeId.VARCHAR_NAME) ||
                    typeName.equals(TypeId.CLOB_NAME)) ||
                    typeName.equals(TypeId.LONGVARCHAR_NAME));
        }
    }

    /**
     * Creates a UTF-8 stream for an application reader whose length isn't
     * known at insertion time.
     * <p>
     * The application reader is coming in through one of the "lengthless
     * overrides" added in JDBC 4.0, for instance
     * java.sql.PreparedStatement.setCharacterStream(int,Reader).
     * A limit is placed on the length of the application reader. If the reader
     * exceeds the maximum length, truncation of trailing blanks is attempted.
     * If truncation fails, an exception is thrown.
     *
     * @param appReader application reader
     * @param maximumLength maximum allowed length in number of characters for
     *      the reader, typically the maximum field size
     * @param typeName type name of the column data is inserted into
     * @param headerGenerator the stream header generator
     * @throws IllegalArgumentException if maximum length is negative
     */
    public ReaderToUTF8Stream(Reader appReader,
                              int maximumLength,
                              String typeName,
                              StreamHeaderGenerator headerGenerator) {
        this(appReader, -1 * maximumLength, 0, typeName, headerGenerator);
        if (maximumLength < 0) {
            throw new IllegalArgumentException("Maximum length for a capped " +
                    "stream cannot be negative: " + maximumLength);
        }
        reader.setLimit(maximumLength);
    }

    /**
     * Reads a byte from the stream.
     * <p>
     * Characters read from the source stream are converted to the UTF-8 Derby
     * specific encoding.
     *
     * @return The byte read, or {@code -1} if the end-of-stream is reached.
     * @throws EOFException if the end-of-stream has already been reached or
     *      the stream has been closed
     * @throws IOException if reading from the source stream fails
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
            fillBuffer(FIRST_READ);

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

    /**
     * Reads up to {@code len} bytes from the stream.
     * <p>
     * Characters read from the source stream are converted to the UTF-8 Derby
     * specific encoding.
     *
     * @return The number of bytes read, or {@code -1} if the end-of-stream is
     *      reached.
     * @throws EOFException if the end-of-stream has already been reached or
     *      the stream has been closed
     * @throws IOException if reading from the source stream fails
     * @see java.io.InputStream#read(byte[],int,int)
     */
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
            fillBuffer(FIRST_READ);

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

    /**
     * Fills the internal buffer with data read from the source stream.
     * <p>
     * The characters read from the source are converted to the modified UTF-8
     * encoding, used as the on-disk format by Derby.
     *
     * @param startingOffset offset at which to start filling the buffer, used
     *      to avoid overwriting the stream header data on the first iteration
     * @throws DerbyIOException if the source stream has an invalid length
     *      (different than specified), or if truncation of blanks fails
     * @throws IOException if reading from the source stream fails
     */
	private void fillBuffer(int startingOffset) throws IOException
	{
        if (startingOffset == FIRST_READ) {
            // Generate the header. Provide the char length only if the header
            // encodes a char count and we actually know the char count.
            if (hdrGen.expectsCharCount() && valueLength >= 0) {
                headerLength = hdrGen.generateInto(buffer, 0, valueLength);
            } else {
                headerLength = hdrGen.generateInto(buffer, 0, -1);
            }
            // Make startingOffset point at the first byte after the header.
            startingOffset = headerLength;
        }
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
            charCount++; // Increment the character count.

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
                                SQLState.LANG_STRING_TRUNCATION,
                                typeName,
                                "<stream-value>", // Don't show the whole value.
                                String.valueOf(Math.abs(valueLength))),
                            SQLState.LANG_STRING_TRUNCATION);
                }
            }
        }

        // can put the correct length into the stream.
        if (!multipleBuffer) {
            int newValueLen = -1;
            if (hdrGen.expectsCharCount()) {
                if (SanityManager.DEBUG && charCount == 0) {
                    SanityManager.ASSERT(eof);
                }
                newValueLen = charCount;
            } else {
                // Store the byte length of the user data (exclude the header).
                newValueLen = blen - headerLength;
            }
            int newHeaderLength = hdrGen.generateInto(buffer, 0, newValueLen);
            // Check that we didn't overwrite any of the user data.
            if (newHeaderLength != headerLength) {
                throw new IOException("Data corruption detected; user data " +
                        "overwritten by header bytes");
            }
            // Write the end-of-stream marker (if required).
            blen += hdrGen.writeEOF(buffer, blen, newValueLen);
        } else {
            // Write the end-of-stream marker (if required).
            blen += hdrGen.writeEOF(buffer, blen, Math.max(valueLength, -1));
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
        } else if (typeName.equals(TypeId.CHAR_NAME)) {
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
                        "<stream-value>", // Don't show the whole value.
                        String.valueOf(Math.abs(valueLength))),
                    SQLState.LANG_STRING_TRUNCATION);
            }
        }
    }

    /**
     * return resources 
     */
    public void close() {
        // since stream has been read and eof reached, return buffer back to 
        // the vm.
        // Instead of using another variable to indicate stream is closed
        // a check on (buffer==null) is used instead. 
        buffer = null;
	}

    /**
     * Return an optimized version of bytes available to read from 
     * the stream.
     * <p>
     * Note, it is not exactly per {@code java.io.InputStream#available()}.
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
