/*

   Derby - Class org.apache.derby.iapi.services.classfile.CONSTANT_Index_info

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
