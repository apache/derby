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

/** Double Constant - page 97 - Section 4.4.5 */
final class CONSTANT_Double_info extends ConstantPoolEntry {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	private final double value;

	CONSTANT_Double_info(double value) {
		super(VMDescriptor.CONSTANT_Double);
		doubleSlot = true; //See page 98.
		this.value = value;
	}

	public int hashCode() {
		return (int) value;
	}

	int classFileSize() {
		// 1 (tag) + 8 (double length)
		return 1 + 8;
	}
	void put(ClassFormatOutput out) throws IOException {
		super.put(out);
		out.writeDouble(value);
	}

	public boolean equals(Object other) {

		// check it is the right type
		if (other instanceof CONSTANT_Double_info) {
		
			return value == ((CONSTANT_Double_info) other).value;
		}

		return false;
	}
}


