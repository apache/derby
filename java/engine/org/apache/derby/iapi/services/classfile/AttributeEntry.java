/*

   Derby - Class org.apache.derby.iapi.services.classfile.AttributeEntry

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

import java.io.IOException;

class AttributeEntry {

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
