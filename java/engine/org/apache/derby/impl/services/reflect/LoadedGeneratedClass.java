/*

   Derby - Class org.apache.derby.impl.services.reflect.LoadedGeneratedClass

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

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.services.loader.GeneratedByteCode;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.loader.ClassInfo;


public abstract class LoadedGeneratedClass
	implements GeneratedClass
{

	/*
	** Fields
	*/

	private final ClassInfo	ci;
	private final int classLoaderVersion;

	/*
	**	Constructor
	*/

	public LoadedGeneratedClass(ClassFactory cf, Class jvmClass) {
		ci = new ClassInfo(jvmClass);
		classLoaderVersion = cf.getClassLoaderVersion();
	}

	/*
	** Public methods from Generated Class
	*/

	public String getName() {
		return ci.getClassName();
	}

	public Object newInstance(Context context) throws StandardException	{

		Throwable t;
		try {
			GeneratedByteCode ni =  (GeneratedByteCode) ci.getNewInstance();
			ni.initFromContext(context);
			ni.setGC(this);
			ni.postConstructor();
			return ni;

		} catch (InstantiationException ie) {
			t = ie;
		} catch (IllegalAccessException iae) {
			t = iae;
		} catch (java.lang.reflect.InvocationTargetException ite) {
			t = ite;
		} catch (LinkageError le) {
			t = le;
		}

		throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
	}

	public final int getClassLoaderVersion() {
		return classLoaderVersion;
	}

	/*
	** Methods for subclass
	*/
	protected Class getJVMClass() {
		return ci.getClassObject();
	}
}
