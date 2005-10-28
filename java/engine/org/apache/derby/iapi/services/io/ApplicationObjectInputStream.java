/*

   Derby - Class org.apache.derby.iapi.services.io.ApplicationObjectInputStream

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.loader.ClassFactory;

import java.io.ObjectStreamClass;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
	An object input stream that implements resolve class in order
	to load the class through the ClassFactory.loadApplicationClass method.
*/
class ApplicationObjectInputStream extends ObjectInputStream
    implements ErrorObjectInput
{

	protected ClassFactory cf;
	protected ObjectStreamClass        initialClass;

	ApplicationObjectInputStream(InputStream in, ClassFactory cf)
		throws IOException {
		super(in);
		this.cf = cf;
	}

	protected Class resolveClass(ObjectStreamClass v)
		throws IOException, ClassNotFoundException {

		if (initialClass == null)
			initialClass = v;

		if (cf != null)
			return cf.loadApplicationClass(v);

		throw new ClassNotFoundException(v.getName());
	}

	public String getErrorInfo() {
		if (initialClass == null)
			return "";

		return initialClass.getName() + " (serialVersionUID="
			+ initialClass.getSerialVersionUID() + ")";
	}

	public Exception getNestedException() {
        return null;
	}

}
