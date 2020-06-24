/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassFormatOutput

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
    of the types defined on pages 83 of the Java Virtual Machine spec.

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
		this(new AccessibleByteArrayOutputStream(size));
	}
	public ClassFormatOutput(java.io.OutputStream stream) {
		super(stream);
	}
	public void putU1(int i) throws IOException {
		// ensure the format of the class file is not
		// corrupted by writing an incorrect, truncated value.
//IC see: https://issues.apache.org/jira/browse/DERBY-176
		if (i > 255)
			ClassFormatOutput.limit("U1", 255, i);
		write(i);
	}
	public void putU2(int i) throws IOException {
		putU2("U2", i);

	}
	public void putU2(String limit, int i) throws IOException {
		
		// ensure the format of the class file is not
		// corrupted by writing an incorrect, truncated value.
		if (i > 65535)
			ClassFormatOutput.limit(limit, 65535, i);
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

	/**
	 * Throw an ClassFormatError if a limit of the Java class file format is reached.
	 * @param name Terse limit description from JVM spec.
	 * @param limit What the limit is.
	 * @param value What the value for the current class is
	 * @throws IOException Thrown when limit is exceeded.
	 */
	static void limit(String name, int limit, int value)
//IC see: https://issues.apache.org/jira/browse/DERBY-176
		throws IOException
	{
		throw new IOException(name + "(" + value + " > " + limit + ")");
	}
}
