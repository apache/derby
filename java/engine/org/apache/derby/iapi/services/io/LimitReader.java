/*

   Derby - Class org.apache.derby.iapi.services.io.LimitReader

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.io.Reader;
import java.io.IOException;

/**
	A  Reader that provides methods to limit the range that
	can be read from the reader.
*/
public class LimitReader extends Reader implements Limit 
{
	protected int remainingBytes;
	protected boolean limitInPlace;
	private	Reader	reader;

	/**
		Construct a LimitReader and call the clearLimit() method.
	*/
	public LimitReader(Reader reader) 
	{
		super();
		this.reader = reader;
		clearLimit();
	}

	public int read() throws IOException 
	{

		if (!limitInPlace)
			return reader.read();
		
		if (remainingBytes == 0)
			return -1; // end of file

		
		int value = reader.read();
		if (value >= 0)
			remainingBytes--;
		return value;

	}

	public int read(char c[], int off, int len) throws IOException 
	{
		if (!limitInPlace)
			return reader.read(c, off, len);

		if (remainingBytes == 0)
			return -1;

		if (remainingBytes < len) 
		{
			len = remainingBytes; // end of file
		}

		len = reader.read(c, off, len);
		if (len >= 0)
			remainingBytes -= len;
		return len;
	}

	public long skip(long count)
		throws IOException 
	{
		if (!limitInPlace)
			return reader.skip(count);

		if (remainingBytes == 0)
			return 0; // end of file

		if (remainingBytes < count)
			count = remainingBytes;

		count = reader.skip(count);
		remainingBytes -= count;
		return count;
	}

	public void close()
		throws IOException 
	{
		reader.close();
	}

	/**
		Set the limit of the stream that can be read. After this
		call up to and including length bytes can be read from or skipped in
		the stream. Any attempt to read more than length bytes will
		result in an EOFException

		@return The value of length.
		@exception IOException IOException from some underlying stream
		@exception EOFException The set limit would exceed
		the available data in the stream.
	*/
	public void setLimit(int length) 
	{
		remainingBytes = length;
		limitInPlace = true;
		return;
	}

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read.
		-1 if not limit was set.
	*/
	public int clearLimit() 
	{
		int leftOver = remainingBytes;
		limitInPlace = false;
		remainingBytes = -1;
		return leftOver;
	}
}
