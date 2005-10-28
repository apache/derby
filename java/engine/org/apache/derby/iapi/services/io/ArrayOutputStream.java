/*

   Derby - Class org.apache.derby.iapi.services.io.ArrayOutputStream

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

import java.io.IOException;
import java.io.EOFException;
import java.io.OutputStream;

public class ArrayOutputStream extends OutputStream implements Limit {

	private byte[] pageData;

	private int		start;
	private int		end;		// exclusive
	private int		position;

	public ArrayOutputStream() {
		super();
	}

	public ArrayOutputStream(byte[] data) {
		super();
		setData(data);
	}

	public void setData(byte[] data) {
		pageData = data;
		start = 0;
		if (data != null)
			end = data.length;
		else
			end = 0;
		position = 0;

	}

	/*
	** Methods of OutputStream
	*/

	public void write(int b) throws IOException {
		if (position >= end)
			throw new EOFException();

		pageData[position++] = (byte) b;

	}

	public void write(byte b[], int off, int len) throws IOException {

		if ((position + len) > end)
			throw new EOFException();

		System.arraycopy(b, off, pageData, position, len);
		position += len;
	}

	/*
	** Methods of LengthOutputStream
	*/

	public int getPosition() {
		return position;
	}

	/**
		Set the position of the stream pointer.
	*/
	public void setPosition(int newPosition)
		throws IOException {
		if ((newPosition < start) || (newPosition > end))
			throw new EOFException();

		position = newPosition;
	}

	public void setLimit(int length) throws IOException {

		if (length < 0) {
			throw new EOFException();
		}

		if ((position + length) > end) {
			throw new EOFException();
		}

		start = position;
		end = position + length;

		return;
	}

	public int clearLimit() {

		int unwritten = end - position;

		end = pageData.length;

		return unwritten;
	}
}
