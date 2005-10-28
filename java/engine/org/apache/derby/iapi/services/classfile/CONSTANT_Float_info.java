/*

   Derby - Class org.apache.derby.iapi.services.classfile.CONSTANT_Float_info

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


/** Float Constant - page 96 */
final class CONSTANT_Float_info extends ConstantPoolEntry {
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

