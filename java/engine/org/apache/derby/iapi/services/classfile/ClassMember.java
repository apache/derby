/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassMember

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

import java.lang.reflect.Modifier;

import java.io.IOException;



public class ClassMember {
	protected ClassHolder cpt;
	protected int access_flags;
	protected int name_index;
	protected int descriptor_index;
	protected Attributes attribute_info; // can be null

	ClassMember(ClassHolder cpt, int modifier, int name, int descriptor) {
		this.cpt = cpt;
		name_index = name;
		descriptor_index = descriptor;
		access_flags = modifier;
	}

	/*
	** Public methods from ClassMember
	*/

    public int getModifier() {
			return access_flags;
	}

    public String getDescriptor() {
		return cpt.nameIndexToString(descriptor_index);
	}
	
	public String getName() {
		return cpt.nameIndexToString(name_index);
	}

	public void addAttribute(String attributeName, ClassFormatOutput info) {

		if (attribute_info == null)
			attribute_info = new Attributes(1);

		attribute_info.addEntry(new AttributeEntry(cpt.addUtf8(attributeName), info));
	}


	/*
	**	 ----
	*/

	void put(ClassFormatOutput out) throws IOException {
		out.putU2(access_flags);
		out.putU2(name_index);
		out.putU2(descriptor_index);

		if (attribute_info != null) {
			out.putU2(attribute_info.size());
			attribute_info.put(out);
		} else {
			out.putU2(0);
		}
	}

	int classFileSize() {
		int size = 2 + 2 + 2 + 2;
		if (attribute_info != null)
			size += attribute_info.classFileSize();
		return size;
	}
}
