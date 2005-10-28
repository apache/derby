/*

   Derby - Class org.apache.derby.impl.services.reflect.ReflectMethod

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

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

class ReflectMethod implements GeneratedMethod {

	private final Method	realMethod;

	ReflectMethod(Method m) {
		super();
		realMethod = m;
	}

	public Object invoke(Object ref)
		throws StandardException {

		Throwable t;

		try {
			return realMethod.invoke(ref, null);

		} catch (IllegalAccessException iae) {

			t = iae;

		} catch (IllegalArgumentException iae2) {

			t = iae2;

		} catch (InvocationTargetException ite) {

			t = ite.getTargetException();
			if (t instanceof StandardException)
				throw (StandardException) t;
		}
		
		throw StandardException.unexpectedUserException(t);
	}


}
