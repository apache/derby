/*

   Derby - Class org.apache.derby.iapi.services.io.LimitInputStream

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
import java.io.FilterInputStream;
import java.io.IOException;

/**
	An abstract InputStream that provides abstract methods to limit the range that
	can be read from the stream.
*/
public class LimitInputStream extends FilterInputStream implements Limit {

	protected int remainingBytes;
	protected boolean limitInPlace;

	/**
		Construct a LimitInputStream and call the clearLimit() method.
	*/
	public LimitInputStream(InputStream in) {
		super(in);
		clearLimit();
	}

	public int read() throws IOException {

		if (!limitInPlace)
			return super.read();
		
		if (remainingBytes == 0)
			return -1; // end of file

		
		int value = super.read();
		if (value >= 0)
			remainingBytes--;
		return value;

	}

	public int read(byte b[], int off, int len) throws IOException {

		if (!limitInPlace)
			return super.read(b, off, len);


		if (remainingBytes == 0)
			return -1;

		if (remainingBytes < len) {
			len = remainingBytes; // end of file
		}

		len = super.read(b, off, len);
		if (len > 0)
			remainingBytes -= len;

		return len;
	}

	public long skip(long count)  throws IOException {
		if (!limitInPlace)
			return super.skip(count);

		if (remainingBytes == 0)
			return 0; // end of file

		if (remainingBytes < count)
			count = remainingBytes;

		count = super.skip(count);
		remainingBytes -= count;
		return count;
	}

	public int available() throws IOException {

		if (!limitInPlace)
			return super.available();

		if (remainingBytes == 0)
			return 0; // end of file

		int actualLeft = super.available();

		if (remainingBytes < actualLeft)
			return remainingBytes;
		

		return actualLeft;
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
	public void setLimit(int length) {
		remainingBytes = length;
		limitInPlace = true;
		return;
	}

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read.
		-1 if no limit was set.
	*/
	public int clearLimit() {
		int leftOver = remainingBytes;
		limitInPlace = false;
		remainingBytes = -1;
		return leftOver;
	}

	public void setInput(InputStream in) {
		this.in = in;
	}
}
