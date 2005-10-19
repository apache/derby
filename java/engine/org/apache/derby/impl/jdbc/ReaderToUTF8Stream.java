/*

   Derby - Class org.apache.derby.impl.jdbc.ReaderToUTF8Stream

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
import java.io.IOException;
import java.io.EOFException;
import java.io.Reader;
import java.io.UTFDataFormatException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.LimitReader;
import org.apache.derby.iapi.types.TypeId;

/**
	Converts a java.io.Reader to the on-disk UTF8 format used by Cloudscape.
*/
final class ReaderToUTF8Stream
	extends InputStream
{

	private LimitReader reader;

	private byte[] buffer;
	private int boff;
	private int blen;
	private boolean eof;
	private boolean multipleBuffer;
    // buffer to hold the data read from stream 
    // and converted to UTF8 format
    private final static int BUFSIZE = 32768;
    
    // Number of characters to truncate from this stream
    // The SQL standard allows for truncation of trailing spaces 
    // for clobs,varchar,char.
    private int charsToTruncate;
    private final char SPACE =' ';
    
    // this stream needs to fit into a column of colWidth
    // if truncation error happens ,then the error message includes 
    // information about the column width which is why this variable
    // is needed.
    private int colWidth;  
    
	ReaderToUTF8Stream(LimitReader reader,int length,int numCharsToTruncate)
	{
		this.reader = reader;
		buffer = new byte[BUFSIZE];
		blen = -1;
        this.charsToTruncate = numCharsToTruncate;
        this.colWidth = length;
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
		JDBC 3.0 (from tutorial book) requires that an
		input stream has the correct number of bytes in
		the stream.
	*/
	private void checkSufficientData() throws IOException
	{
		// now that we finished reading from the stream; the amount
        // of data that we can insert,start check for trailing spaces
        if (charsToTruncate > 0)
        {
            reader.setLimit(charsToTruncate);
            int c = 0;
            do
            {
                c = reader.read();
                
                if (c < 0)
                {
                    break;
                }
                else if (c != SPACE)
                {
                    // throw truncation error, wont happen here for any other 
                    // type except for clob
                    // hence using TypeId.CLOB_NAME to avoid having to store
                    // the type information along with this stream.
                    throw new IOException(
                        MessageService.getTextMessage(
                            SQLState.LANG_STRING_TRUNCATION,
                            TypeId.CLOB_NAME, 
                            "XXXX", 
                            String.valueOf(colWidth)));
                }
            }
            while (c == SPACE);
        }
        
        int remainingBytes = reader.clearLimit();
		if (remainingBytes > 0)
			throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INEXACT_LENGTH_DATA));

		// if we had a limit try reading one more character.
		// JDBC 3.0 states the stream muct have the correct number of 
        // characters in it.

		if (remainingBytes == 0) {
			int c;
			try
			{
				c = reader.read();
               
			}
			catch (IOException ioe) {
				c = -1;
			}
			if (c >= 0)
				throw new IOException(MessageService.getTextMessage(SQLState.SET_STREAM_INEXACT_LENGTH_DATA));
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

