/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassFormatOutput

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.classfile;

import org.apache.derby.iapi.services.io.AccessibleByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/** A wrapper around DataOutputStream to provide input functions in terms
    of the types defined on pages 83.

	For this types use these methods of DataOutputStream
	<UL>
	<LI>float - writeFloat
	<LI>long - writeLong
	<LI>double - writeDouble
	<LI>UTF/String - writeUTF
	<LI>U1Array - write(byte[])
	</UL>
 */

public final class ClassFormatOutput extends DataOutputStream {

	public ClassFormatOutput() {
		this(512);
	}

	public ClassFormatOutput(int size) {
		super(new AccessibleByteArrayOutputStream(size));
	}

	public void putU1(int i) throws IOException {
		write(i);
	}
	public void putU2(int i) throws IOException {
		write(i >> 8);
		write(i);
	}
	public void putU4(int i) throws IOException {
		writeInt(i);
	}

	public void writeTo(OutputStream outTo) throws IOException {
		((AccessibleByteArrayOutputStream) out).writeTo(outTo);
	}

	/**
		Get a reference to the data array the class data is being built
		in. No copy is made.
	*/
	public byte[] getData() {
		return ((AccessibleByteArrayOutputStream) out).getInternalByteArray();
	}
}
