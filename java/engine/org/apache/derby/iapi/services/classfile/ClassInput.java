/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.classfile;

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.IOException;


/**	A wrapper around DataInputStream to provide input functions in terms
    of the types defined on pages 83.
 */

class ClassInput extends DataInputStream {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	ClassInput(InputStream in) {
		super(in);
	}

	int getU2() throws IOException {
		return readUnsignedShort();
	}
	int getU4() throws IOException {
		return readInt();
	}
	byte[] getU1Array(int count) throws IOException {
		byte[] b = new byte[count];
		readFully(b);
		return b;
	}
}
