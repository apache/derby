/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

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
