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

 /**

  A generic constant pool entry for entries that simply hold indexes
  into other entries.

  <BR>
  Ref Constant Pool Entry  - page 94 - Section 4.4.2	- Two indexes
  <BR>
  NameAndType Constant Pool Entry  - page 99 - Section 4.4.6 - Two indexes
  <BR>
  String Constant Pool Entry - page 96 - Section 4.4.3 - One index
  <BR>
  Class Reference Constant Pool Entry - page 93 - Section 4.4.1 - One index

*/
final class CONSTANT_Index_info extends ConstantPoolEntry {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private int i1;
	private int i2;

	CONSTANT_Index_info(int tag, int i1, int i2) {
		super(tag);
		this.i1 = i1;
		this.i2 = i2;
	}

	public int hashCode() {
		return (tag << 16) | ((i1 << 8) ^ i2);
	}

	public boolean equals(Object other) {
		if (other instanceof CONSTANT_Index_info) {
			CONSTANT_Index_info o = (CONSTANT_Index_info) other;

			return (tag == o.tag) && (i1 == o.i1) && (i2 == o.i2);			
		}
		return false;
	}


	/**
		Used when searching 
	*/
	void set(int tag, int i1, int i2) {
		this.tag = tag;
		this.i1 = i1;
		this.i2 = i2;
	}

	int classFileSize() {
		// 1 (tag) + 2 (index length) [ + 2 (index length) ]
		return 1 + 2 + ((i2 != 0) ? 2 : 0);
	}

	void put(ClassFormatOutput out) throws IOException {
		super.put(out);
		out.putU2(i1);
		if (i2 != 0)
			out.putU2(i2);
	}

	public int getI1() { return i1; }

	public int getI2() { return i2; }
}
