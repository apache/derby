/*

   Derby - Class org.apache.derby.iapi.services.io.CounterOutputStream

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

import java.io.OutputStream;
import java.io.IOException;
import java.io.EOFException;

/**
	An OutputStream that simply provides methods to count the number
	of bytes written to an underlying stream.
*/

public class CounterOutputStream extends OutputStream implements Limit {

	protected OutputStream out;
	private int count;
	private int limit;

	/**
		Create a CounterOutputStream that will discard any bytes
		written but still coutn them and call its reset method
		so that the count is intially zero.
	*/
	public CounterOutputStream() {
		super();
	}

	public void setOutputStream(OutputStream out) {
		this.out = out;
		setLimit(-1);
	}

	/**
		Get count of bytes written to the stream since the last
		reset() call.
	*/
	public int getCount() {
		return count;
	}

	/**
		Set a limit at which an exception will be thrown. This allows callers
		to count the number of bytes up to some point, without having to complete
		the count. E.g. a caller may only want to see if some object will write out
		over 4096 bytes, without waiting for all 200,000 bytes of the object to be written.
		<BR>
		If the passed in limit is 0 or negative then the stream will count bytes without
		throwing an exception.

		@see EOFException
	*/
	public void setLimit(int limit) {

		count = 0;

		this.limit = limit;

		return;
	}

	public int clearLimit() {

		int unused = limit - count;
		limit = 0;

		return unused;
	}

	/*
	** Methods of OutputStream
	*/

	/**
		Add 1 to the count.

		@see OutputStream#write
	*/
	public  void write(int b) throws IOException {
		
		if ((limit >= 0) && ((count + 1) > limit)) {
			throw new EOFException();
		}

		out.write(b);
		count++;
	}

	/**
		Add b.length to the count.

		@see OutputStream#write
	*/
	public void write(byte b[]) throws IOException {
		
		if ((limit >= 0) && ((count + b.length) > limit)) {
			throw new EOFException();
		}

		out.write(b);
		count += b.length;
	}

	/**
		Add len to the count, discard the data.

		@see OutputStream#write
	*/
	public void write(byte b[], int off, int len) throws IOException {

		if ((limit >= 0) && ((count + len) > limit)) {
			throw new EOFException();
		}

		out.write(b, off, len);
		count += len;
	}
}
