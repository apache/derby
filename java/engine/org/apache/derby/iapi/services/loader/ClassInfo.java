/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassInfo

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

package org.apache.derby.iapi.services.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ClassInfo implements InstanceGetter {

	private static final Class[] noParameters = new Class[0];
	private static final Object[] noArguments = new Object[0];

	private final Class clazz;
	private boolean useConstructor = true;
	private Constructor noArgConstructor;

	public ClassInfo(Class clazz) {
		this.clazz = clazz;
	}

	/**
		Return the name of this class.
	*/
	public final String getClassName() {
		return clazz.getName();
	}

	/**
		Return the class object for this class.

	*/
	public final Class getClassObject() {

		return clazz;
	}

	/**
		Create an instance of this class. Assumes that clazz has already been
		initialized. Optimizes Class.newInstance() by caching and using the
		no-arg Constructor directly. Class.newInstance() looks up the constructor
		each time.

		@exception InstantiationException Zero arg constructor can not be executed
		@exception IllegalAccessException Class or zero arg constructor is not public.
		@exception InvocationTargetException Exception throw in zero-arg constructor.

	*/
	public Object getNewInstance()
		throws InstantiationException, IllegalAccessException, InvocationTargetException  {

		if (!useConstructor) {

			return clazz.newInstance();
		}

		if (noArgConstructor == null) {

			try {
				noArgConstructor =  clazz.getConstructor(noParameters);

			} catch (NoSuchMethodException nsme) {
				// let Class.newInstace() generate the exception
				useConstructor = false;
				return getNewInstance();

			} catch (SecurityException se) {
				// not allowed to to get a handle on the constructor
				// just use the standard mechanism.
				useConstructor = false;
				return getNewInstance();
			}
		}

		try {
			return noArgConstructor.newInstance(noArguments);
		} catch (IllegalArgumentException iae) {
			// can't happen since no arguments are passed.
			return null;
		}
	}
}
