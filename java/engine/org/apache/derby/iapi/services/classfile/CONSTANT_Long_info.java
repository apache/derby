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

/** Long Constant - page 97 - Section 4.4.5 */
final class CONSTANT_Long_info extends ConstantPoolEntry {
	private final long value;

	CONSTANT_Long_info(long value) {
		super(VMDescriptor.CONSTANT_Long);
		doubleSlot = true; //See page 98.
		this.value = value;
	}

	public int hashCode() {
		return (int) value;
	}

	public boolean equals(Object other) {

		// check it is the right type
		if (other instanceof CONSTANT_Long_info) {
		
			return value == ((CONSTANT_Long_info) other).value;
		}

		return false;
	}

	int classFileSize() {
		// 1 (tag) + 8 (long length)
		return 1 + 8;
	}

	void put(ClassFormatOutput out) throws IOException {
		super.put(out);
		out.writeLong(value);
	}
}

