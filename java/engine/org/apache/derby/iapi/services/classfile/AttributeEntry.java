/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.classfile;

import java.io.IOException;

class AttributeEntry {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private int attribute_name_index;
	private ClassFormatOutput infoOut;
	byte[] infoIn;

	AttributeEntry(int name_index, ClassFormatOutput info) {
		super();

		attribute_name_index = name_index;
		this.infoOut = info;
	}


	AttributeEntry(ClassInput in) throws IOException {
		attribute_name_index = in.getU2();
		infoIn = in.getU1Array(in.getU4());
	}

	int getNameIndex() { return attribute_name_index; }

	void put(ClassFormatOutput out) throws IOException {
		out.putU2(attribute_name_index);
		if (infoOut != null) {
			out.putU4(infoOut.size());
			infoOut.writeTo(out);
		} else {
			out.putU4(infoIn.length);
			out.write(infoIn);
		}
	}

	/**
		This is exact.
	*/
	int classFileSize() {
		return 2 + 4 + 
			((infoOut != null) ? infoOut.size() : infoIn.length);
	}
}
