/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ClassInfo implements InstanceGetter {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
