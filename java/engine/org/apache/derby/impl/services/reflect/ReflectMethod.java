/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

class ReflectMethod implements GeneratedMethod {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
