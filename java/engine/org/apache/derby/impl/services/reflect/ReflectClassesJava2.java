/*

   Derby - Class org.apache.derby.impl.services.reflect.ReflectClassesJava2

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

package org.apache.derby.impl.services.reflect;
import org.apache.derby.iapi.util.ByteArray;

/**
	Relfect loader with Privileged block for Java 2 security. 
*/

public class ReflectClassesJava2 extends DatabaseClasses
	implements java.security.PrivilegedAction<Object>
{

	private java.util.HashMap<String,ReflectGeneratedClass> preCompiled;

	private int action = -1;

	synchronized LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump) {

		if (classDump == null || classDump.getArray() == null) {

			if (preCompiled == null)
				preCompiled = new java.util.HashMap<String,ReflectGeneratedClass>();
			else
			{
				ReflectGeneratedClass gc = preCompiled.get(fullyQualifiedName);
				if (gc != null)
					return gc;
			}

			// not a generated class, just load the class directly.
			try {
				Class jvmClass = Class.forName(fullyQualifiedName);
				ReflectGeneratedClass gc = new ReflectGeneratedClass(this, jvmClass);
				preCompiled.put(fullyQualifiedName, gc);
				return gc;
			} catch (ClassNotFoundException cnfe) {
				throw new NoClassDefFoundError(cnfe.toString());
			}
		}

		action = 1;
		return ((ReflectLoaderJava2) java.security.AccessController.doPrivileged(this)).loadGeneratedClass(fullyQualifiedName, classDump);
	}

	public final Object run() {

		try {
			// SECURITY PERMISSION - MP2
			switch (action) {
			case 1:
				return new ReflectLoaderJava2(getClass().getClassLoader(), this);
			case 2:
				return Thread.currentThread().getContextClassLoader();
			default:
				return null;
			}
		} finally {
			action = -1;
		}
		
	}

	Class loadClassNotInDatabaseJar(String name) throws ClassNotFoundException {
		
		Class foundClass = null;
		
	    // We may have two problems with calling  getContextClassLoader()
	    // when trying to find our own classes for aggregates.
	    // 1) If using the URLClassLoader a ClassNotFoundException may be 
	    //    thrown (Beetle 5002).
	    // 2) If Derby is loaded with JNI, getContextClassLoader()
	    //    may return null. (Beetle 5171)
	    //
	    // If this happens we need to user the class loader of this object
	    // (the classLoader that loaded Derby). 
	    // So we call Class.forName to ensure that we find the class.
        try {
        	ClassLoader cl;
        	synchronized(this) {
        	  action = 2;
              cl = ((ClassLoader)
			      java.security.AccessController.doPrivileged(this));
        	}
			
			foundClass = (cl != null) ?  cl.loadClass(name) 
				      :Class.forName(name);
        } catch (ClassNotFoundException cnfe) {
            foundClass = Class.forName(name);
        }
		return foundClass;
	}
}
