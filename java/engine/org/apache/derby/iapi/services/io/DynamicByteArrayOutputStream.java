/*

   Derby - Class org.apache.derby.iapi.services.io.DynamicByteArrayOutputStream

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
	A DynamicByteArrayOutputStream allows writing to a dynamically resizable
	array of bytes.   In addition to dynamic resizing, this extension allows
	the user of this class to have more control over the position of the stream
	and can get a direct reference of the array.
*/
public class DynamicByteArrayOutputStream extends OutputStream {

	private static int INITIAL_SIZE = 4096;

	private byte[] buf;
	private int		position;
	private int		used;		// how many bytes are used
	private int		beginPosition;

	public DynamicByteArrayOutputStream() {
		this(INITIAL_SIZE);
	}

	public DynamicByteArrayOutputStream(int size) {
		super();

		buf = new byte[size];
	}

	public DynamicByteArrayOutputStream(byte[] data) {
		super();

		buf = data;
	}

	public DynamicByteArrayOutputStream(DynamicByteArrayOutputStream toBeCloned) {

		byte[] cbuf = toBeCloned.getByteArray();
		buf = new byte[cbuf.length];

		write(cbuf, 0, cbuf.length);
		position = toBeCloned.getPosition();
		used = toBeCloned.getUsed();
		beginPosition = toBeCloned.getBeginPosition();
	}

	/*
	 *	OutputStream methods
	 */
	public void write(int b) 
	{
		if (position >= buf.length)
			expandBuffer(INITIAL_SIZE);

		buf[position++] = (byte) b;

		if (position > used)
			used = position;
	}
	
	public void write(byte[] b, int off, int len) 
	{
		if ((position+len) > buf.length)
			expandBuffer(len);

		System.arraycopy(b, off, buf, position, len);
		position += len;

		if (position > used)
			used = position;
	}

	void writeCompleteStream(InputStream dataIn, int len) throws IOException
	{
		if ((position+len) > buf.length)
			expandBuffer(len);

		org.apache.derby.iapi.services.io.InputStreamUtil.readFully(dataIn, buf, position, len);
		position += len;

		if (position > used)
			used = position;
	}

	public void close()
	{
		buf = null;
		reset();
	}

	/*
	 *	Specific methods
	 */

	/**
		Reset the stream for reuse
	*/
	public void reset()
	{
		position = 0;
		beginPosition = 0;
		used = 0;
	}

	/**
		Get a reference to the byte array stored in the byte array output
		stream. Note that the byte array may be longer that getPosition().
		Bytes beyond and including the current poistion are invalid.
	*/
	public byte[] getByteArray()
	{
		return buf;
	}

	/**
		Get the number of bytes that was used.
	*/
	public int getUsed()
	{
		return used;
	}

	/**
		Get the current position in the stream
	*/
	public int getPosition()
	{
		return position;
	}

	/**
		Get the current position in the stream
	*/
	public int getBeginPosition()
	{
		return beginPosition;
	}

	/**
		Set the position of the stream pointer.
		It is up to the caller to make sure the stream has no gap of garbage in
		it or useful information is not left out at the end because the stream
		does not remember anything about the previous position.
	*/
	public void setPosition(int newPosition)
	{
		if (newPosition > position)
		{
			if (newPosition > buf.length)
				expandBuffer(newPosition - buf.length);
		}

		position = newPosition;

		if (position > used)
			used = position;

		return ;
	}

	/**
		Set the begin position of the stream pointer.
		If the newBeginPosition is larger than the stream itself,
		then, the begin position is not set.
	*/
	public void setBeginPosition(int newBeginPosition)
	{

		if (newBeginPosition > buf.length)
			return;

		beginPosition = newBeginPosition;
	}

	/**
		Shrink the buffer left by the amount given. Ie.
		bytes from 0 to amountToShrinkBy are thrown away
	*/
	public void discardLeft(int amountToShrinkBy) {

		System.arraycopy(buf, amountToShrinkBy, buf, 0,
			used - amountToShrinkBy);

		position -= amountToShrinkBy;
		used -= amountToShrinkBy;
	}

	/**
		Expand the buffer by at least the number of bytes requested in minExtension.

		To optimize performance and reduce memory copies and allocation, we have a staged buffer
		expansion.

		<UL>
		<LI> buf.length < 128k - increase by 4k
		<LI> buf.length < 1Mb - increase by 128k
		<LI> otherwise increase by 1Mb.
		</UL>

		In all cases, if minExpansion is greater than the value about then the buffer will
		be increased by minExtension.
	*/
	private void expandBuffer(int minExtension)
	{
		if (buf.length < (128 * 1024)) {
			if (minExtension < INITIAL_SIZE)
				minExtension = INITIAL_SIZE;
		} else if (buf.length < (1024 * 1024)) {

			if (minExtension < (128 * 1024))
				minExtension = (128 * 1024);
		} else {
			if (minExtension < (1024 * 1024))
				minExtension = 1024 * 1024;
		}

		int newsize = buf.length + minExtension;

		byte[] newbuf = new byte[newsize];
		System.arraycopy(buf, 0, newbuf, 0, buf.length);
		buf = newbuf;
	}

}
