/*

   Derby - Class org.apache.derby.impl.services.bytecode.GClass

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

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.compiler.ClassBuilder;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.util.ByteArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This is a common superclass for the various impls.
 * Saving class files is a common thing to do.
 *
 * @author ames
 */
public abstract class GClass implements ClassBuilder {

	protected ByteArray bytecode;
	protected final ClassFactory cf;
	protected final String qualifiedName;



	public GClass(ClassFactory cf, String qualifiedName) {
		this.cf = cf;
		this.qualifiedName = qualifiedName;
	}
	public String getFullName() {
		return qualifiedName;
	}
	public GeneratedClass getGeneratedClass() throws StandardException {
		return cf.loadGeneratedClass(qualifiedName, getClassBytecode());
	}

	protected void writeClassFile(String dir, boolean logMessage, Throwable t) {

		if (SanityManager.DEBUG) {

		if (bytecode ==  null) getClassBytecode(); // not recursing...

		if (dir == null) dir="";

		String filename = getName(); // leave off package

		filename = filename + ".class";

		File classFile = new File(dir,filename);

		// find the error stream
		HeaderPrintWriter errorStream = Monitor.getStream();

		try {
			FileOutputStream fis = new FileOutputStream(classFile);
			fis.write(bytecode.getArray(),
				bytecode.getOffset(), bytecode.getLength());
			fis.flush();
			if (logMessage) {
				errorStream.printlnWithHeader("Wrote class "+getFullName()+" to file "+classFile.toString()+". Please provide support with the file and the following exception message: "+t);
			}
			fis.close();
		} catch (IOException e) {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unable to write .class file");
		}
		}
	}

	public final void validateType(String typeName1)
	{
	    if (SanityManager.DEBUG)
	    {
            SanityManager.ASSERT(typeName1 != null);

            String typeName = typeName1.trim();

	        // first remove all array-ness
	        while (typeName.endsWith("[]")) typeName = typeName.substring(0,typeName.length()-2);

            SanityManager.ASSERT(typeName.length() > 0);

	        // then check for primitive types
	        if ("boolean".equals(typeName)) return;
	        if ("byte".equals(typeName)) return;
	        if ("char".equals(typeName)) return;
	        if ("double".equals(typeName)) return;
	        if ("float".equals(typeName)) return;
	        if ("int".equals(typeName)) return;
	        if ("long".equals(typeName)) return;
	        if ("short".equals(typeName)) return;
	        if ("void".equals(typeName)) return;

	        // then see if it can be found
	        // REVISIT: this will fail if ASSERT is on and the
	        // implementation at hand is missing the target type.
	        // We do plan to generate classes against
	        // different implementations from the compiler's implementation
	        // at some point...
			try {
				if (cf == null)
					Class.forName(typeName);
				else
					cf.loadApplicationClass(typeName);
			} catch (ClassNotFoundException cnfe) {
				SanityManager.THROWASSERT("Class "+typeName+" not found");
			}

	        // all the checks succeeded, it must be okay.
	        return;
	    }
	}
}
