/*

   Derby - Class org.apache.derby.iapi.services.io.AccessibleByteArrayOutputStream

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
