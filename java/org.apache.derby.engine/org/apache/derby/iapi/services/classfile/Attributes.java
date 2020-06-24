/*

   Derby - Class org.apache.derby.iapi.services.classfile.Attributes

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.services.classfile;

import java.util.Vector;

import java.io.IOException;

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
class Attributes extends Vector<AttributeEntry> {
	private int classFileSize;

	Attributes(int count) {
		super(count);
	}

	void put(ClassFormatOutput out) throws IOException {
		int size = size();
		for (int i = 0; i < size; i++) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			elementAt(i).put(out);
		}
	}

	int classFileSize() {
		return classFileSize;
	}

	/**
	*/

	void addEntry(AttributeEntry item) {
		addElement(item);
		classFileSize += item.classFileSize();
	}
}

