/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;
import org.apache.derby.iapi.util.ByteArray;

/**
	Relfect loader with Privileged block for Java 2 security. 
*/

public final class ReflectClassesJava2 extends DatabaseClasses
	implements java.security.PrivilegedAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private java.util.HashMap preCompiled;

	private int action;

	protected synchronized LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump) {

		if (classDump == null || classDump.getArray() == null) {

			if (preCompiled == null)
				preCompiled = new java.util.HashMap();
			else
			{
				ReflectGeneratedClass gc = (ReflectGeneratedClass) preCompiled.get(fullyQualifiedName);
				if (gc != null)
					return gc;
			}

			// not a generated class, just load the class directly.
			try {
				Class jvmClass = Class.forName(fullyQualifiedName);
				ReflectGeneratedClass gc = new ReflectGeneratedClass(this, jvmClass, null);
				preCompiled.put(fullyQualifiedName, gc);
				return gc;
			} catch (ClassNotFoundException cnfe) {
				throw new NoClassDefFoundError(cnfe.toString());
			}
		}

		action = 1;
		return ((ReflectLoaderJava2) java.security.AccessController.doPrivileged(this)).loadGeneratedClass(fullyQualifiedName, classDump);
	}

	public Object run() {
		// SECURITY PERMISSION - MP2
		switch (action) {
		case 1:
			return new ReflectLoaderJava2(getClass().getClassLoader(), this);
		case 2:
			return Thread.currentThread().getContextClassLoader();
		default:
			return null;
		}
	}

	protected synchronized Class loadClassNotInDatabaseJar(String name) throws ClassNotFoundException {
		
		Class foundClass = null;
		action = 2;
	    // We may have two problems with calling  getContextClassLoader()
	    // when trying to find our own classes for aggregates.
	    // 1) If using the URLClassLoader a ClassNotFoundException may be 
	    //    thrown (Beetle 5002).
	    // 2) If cloudscape is loaded with JNI, getContextClassLoader()
	    //    may return null. (Beetle 5171)
	    //
	    // If this happens we need to user the class loader of this object
	    // (the classLoader that loaded Cloudscape). 
	    // So we call Class.forName to ensure that we find the class.
        try {
            ClassLoader cl = ((ClassLoader)
			      java.security.AccessController.doPrivileged(this));
			
			foundClass = (cl != null) ?  cl.loadClass(name) 
				      :Class.forName(name);
        } catch (ClassNotFoundException cnfe) {
            foundClass = Class.forName(name);
        }
		return foundClass;
	}
}
