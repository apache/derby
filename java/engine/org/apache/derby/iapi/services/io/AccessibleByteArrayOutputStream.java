/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.ByteArrayOutputStream;

/**
 * This allows us to get to the byte array to go back and
 * edit contents or get the array without having a copy made.
 <P>
   Since a copy is not made, users must be careful that no more
   writes are made to the stream if the array reference is handed off.
 * <p>
 * Users of this must make the modifications *before* the
 * next write is done, and then release their hold on the
 * array.
   
 */
public class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {

	public AccessibleByteArrayOutputStream() {
		super();
	}

	public AccessibleByteArrayOutputStream(int size) {
		super(size);
	}

	/**
	 * The caller promises to set their variable to null
	 * before any other calls to write to this stream are made.
	   Or promises to throw away references to the stream before
	   passing the array reference out of its control.
	 */
	public byte[] getInternalByteArray() {
		return buf;
	}
}
