/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassInput

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

import java.io.InputStream;
import java.io.DataInputStream;
import java.io.IOException;


/**	A wrapper around DataInputStream to provide input functions in terms
    of the types defined on pages 83.
 */

class ClassInput extends DataInputStream {

	ClassInput(InputStream in) {
		super(in);
	}

    int getU1() throws IOException {
//IC see: https://issues.apache.org/jira/browse/DERBY-6857
        return readUnsignedByte();
    }

	int getU2() throws IOException {
		return readUnsignedShort();
	}
	int getU4() throws IOException {
		return readInt();
	}
	byte[] getU1Array(int count) throws IOException {
		byte[] b = new byte[count];
		readFully(b);
		return b;
	}
}
