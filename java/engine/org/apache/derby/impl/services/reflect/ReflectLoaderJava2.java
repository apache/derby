/*

   Derby - Class org.apache.derby.impl.services.reflect.ReflectLoaderJava2

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.sql.compile.CodeGeneration;

final class ReflectLoaderJava2 extends ClassLoader {

	/*
	**	Fields
	*/

	private final DatabaseClasses cf;
	
	/*
	** Constructor
	*/

	ReflectLoaderJava2(ClassLoader parent, DatabaseClasses cf) {
		super(parent);
		this.cf = cf;
	}

	protected Class findClass(String name)
		throws ClassNotFoundException {
		return cf.loadApplicationClass(name);
	}

	/*
	** Implementation specific methods
	** NOTE these are COPIED from ReflectLoader as the two classes cannot be made into
	   a super/sub class pair. Because the Java2 one needs to call super(ClassLoader)
	   that was added in Java2 and it needs to not implement loadClass()
	*/

	/**
		Load a generated class from the passed in class data.
	*/
	public LoadedGeneratedClass loadGeneratedClass(String name, ByteArray classData) {

		Class jvmClass = defineClass(name, classData.getArray(), classData.getOffset(), classData.getLength());

		resolveClass(jvmClass);

		/*
			DJD - not enabling this yet, need more memory testing, may only
			create a factory instance when a number of instances are created.
			This would avoid a factory instance for DDL

		// now generate a factory class that loads instances
		int lastDot = name.lastIndexOf('.');
		String factoryName = name.substring(lastDot + 1, name.length()).concat("_F");

		classData = cf.buildSpecificFactory(name, factoryName);
		Class factoryClass = defineClass(CodeGeneration.GENERATED_PACKAGE_PREFIX.concat(factoryName),
			classData.getArray(), classData.getOffset(), classData.getLength());
		resolveClass(factoryClass);
		
		  */
		Class factoryClass = null;

		return new ReflectGeneratedClass(cf, jvmClass, factoryClass);
	}
}
