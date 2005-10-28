/*

   Derby - Class org.apache.derby.iapi.services.classfile.CONSTANT_Integer_info

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

/** Integer Constant - page 96 */
class CONSTANT_Integer_info extends ConstantPoolEntry {
	private final int value;

	CONSTANT_Integer_info(int value) {
		super(VMDescriptor.CONSTANT_Integer);
		this.value = value;
	}

	public int hashCode() {
		return value;
	}

	void put(ClassFormatOutput out) throws IOException {
		super.put(out);
		out.putU4(value);
	}

	public boolean equals(Object other) {

		// check it is the right type
		if (other instanceof CONSTANT_Integer_info) {
		
			return value == ((CONSTANT_Integer_info) other).value;
		}

		return false;
	}

	int classFileSize() {
		// 1 (tag) + 4 (int length)
		return 1 + 4;
	}
}

