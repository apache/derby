/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
