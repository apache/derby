/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.classfile;

import org.apache.derby.iapi.services.classfile.VMDescriptor;
import java.io.IOException;


/** Float Constant - page 96 */
final class CONSTANT_Float_info extends ConstantPoolEntry {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	private final float value;

	CONSTANT_Float_info(float value) {
		super(VMDescriptor.CONSTANT_Float);
		this.value = value;
	}

	public int hashCode() {
		return (int) value;
	}

	public boolean equals(Object other) {

		// check it is the right type
		if (other instanceof CONSTANT_Float_info) {
		
			return value == ((CONSTANT_Float_info) other).value;
		}

		return false;
	}

	int classFileSize() {
		// 1 (tag) + 4 (float length)
		return 1 + 4;
	}

	void put(ClassFormatOutput out) throws IOException {
		super.put(out);
		out.writeFloat(value);
	}
}

