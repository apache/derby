/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.sql.compile.CodeGeneration;
	/**
		IBM Copyright &copy notice.
	*/


final class ReflectLoaderJava2 extends ClassLoader { private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

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
