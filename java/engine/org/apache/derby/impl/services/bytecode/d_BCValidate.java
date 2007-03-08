/*

   Derby - Class org.apache.derby.impl.services.bytecode.d_BCValidate

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

package org.apache.derby.impl.services.bytecode;

import java.lang.reflect.*;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.sanity.SanityManager;
import java.util.Hashtable;
import org.apache.derby.iapi.services.loader.*;
import org.apache.derby.iapi.services.context.*;

/**
 * Validate BC calls.
 *
 */
class d_BCValidate
{

	private static final String[] csPackages = {
		"java",
		"org.apache.derby.exe.",
		"org.apache.derby.iapi.",
		"org.apache.derby.jdbc.",
		"org.apache.derby.iapi.",
		"org.apache.derby.impl.",
		"org.apache.derby.authentication.",
		"org.apache.derby.catalog.",
		"org.apache.derby.iapi.db.",
		"org.apache.derby.iapi.types.",
		"org.apache.derby.iapi.types.",
		"org.apache.derby.catalog.types.",
		};


	private static final Class[] NO_PARAMS = new Class[0];

	static void checkMethod(short opcode, Type dt, String methodName, String[] debugParameterTypes, Type rt) {


		if (SanityManager.DEBUG) {
			String reason = null;
			try {

				String declaringClass = dt.javaName();
				if (declaringClass.startsWith("org.apache.derby.exe."))
					return;

				// only validate against Derby engine or Java classes. Not user defined classes
				int p;
				for (p = 0; p < csPackages.length; p++) {
					if (declaringClass.startsWith(csPackages[p]))
						break;
				}
				if (p == csPackages.length)
					return;

				Class[] params = NO_PARAMS;

				Class declaring = loadClass(declaringClass);

				if (debugParameterTypes != null) {
					params = new Class[debugParameterTypes.length];
					for (int i = 0; i < debugParameterTypes.length; i++) {
						params[i] = loadClass(debugParameterTypes[i]);
					}

				}
				
				// If the class is not in the same class loader then it
				// it must be a non-Derby class. In that case any method etc.
				// being accessed must be public, so don't use the getDeclared
				// methods. Default SecurityManager behaviour is to grant access to public members
				// and members from classes loaded by the same class loader. Thus
				// we try to fall into these categories to avoid having to grant
				// permissions to derby jars for the function tests.
				
				ClassLoader declareLoader = declaring.getClassLoader();
				ClassLoader myLoader = d_BCValidate.class.getClassLoader();
				
				boolean sameClassLoader = false;
				if (declareLoader == myLoader)
					sameClassLoader = true;
				else if (declareLoader != null)
					sameClassLoader = declareLoader.equals(myLoader);
				
				String actualReturnType;

				if (methodName.equals("<init>")) {
					Constructor c;
					
					if (sameClassLoader)
					{
						c = declaring.getDeclaredConstructor(params);
					}
					else
					{
						c = declaring.getConstructor(params);
						
						// check this construct is declared by this
						// class, has to be, right? But no harm checking.
						if (!c.getDeclaringClass().equals(declaring))
						{
							reason = "constructor " + c.toString() + " declared on " + c.getDeclaringClass() + " expected " + declaring;
						}
					}
					
					actualReturnType = "void";
				} else {
					Method m;
					
					if (sameClassLoader)
					{
						m = declaring.getDeclaredMethod(methodName, params);
					}
					else
					{
						m = declaring.getMethod(methodName, params);
						
						// check this method is declared by this
						// class? But no harm checking.
						if (!m.getDeclaringClass().equals(declaring))
						{
							reason = "method " + m.toString() + " declared on " + m.getDeclaringClass() + " expected " + declaring;
						}
					}
					
					actualReturnType = m.getReturnType().getName();
				}
				
				// do we already have a problem?
				if (reason == null)
				{

					Class requestedReturnType = loadClass(rt.javaName());
	
					// check the return type
					if (actualReturnType.equals(requestedReturnType.getName())) {
	
						// check the inteface match
						if (opcode != VMOpcode.INVOKEINTERFACE)
							return;
	
						if (declaring.isInterface())
							return;
	
						reason = "declaring class is not an interface";
	
					} else {
						reason = "return type is " + actualReturnType;
					}
				}


			} catch (Exception e) {
				reason = e.toString();
				e.printStackTrace(System.out);
			}

			String sig = dt.javaName() + " >> " + rt.javaName() + " " + methodName + "(";
			if (debugParameterTypes != null) {
				for (int i = 0; i < debugParameterTypes.length; i++) {
					if (i != 0)
						sig = sig + ", ";
					sig = sig + debugParameterTypes[i];
				}
			}
			sig = sig + ")";

			String msg = "Invalid method " + sig + " because " + reason;

			System.out.println(msg);
			SanityManager.THROWASSERT(msg);
		}
	}

	private static Hashtable primitives;

	static {
		if (SanityManager.DEBUG) {
			primitives = new Hashtable();
			primitives.put("boolean", Boolean.TYPE);
			primitives.put("byte", Byte.TYPE);
			primitives.put("char", Character.TYPE);
			primitives.put("double", Double.TYPE);
			primitives.put("float", Float.TYPE);
			primitives.put("int", Integer.TYPE);
			primitives.put("long", Long.TYPE);
			primitives.put("short", Short.TYPE);
			primitives.put("void", Void.TYPE);
		}

	}
	

	private static Class loadClass(String name) throws ClassNotFoundException {

		if (SanityManager.DEBUG) {

			Class c = (Class) primitives.get(name);
			if (c != null)
				return c;

			if (name.endsWith("[]")) {
				Class baseClass = loadClass(name.substring(0, name.length() - 2));
				return Array.newInstance(baseClass, 0).getClass();
			}
			
			return Class.forName(name);
		}

		return null;
	}
}
