/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.services.loader.GeneratedByteCode;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.context.Context;

import java.lang.reflect.Method;
import java.util.Hashtable;

public final class ReflectGeneratedClass extends LoadedGeneratedClass {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private final Hashtable methodCache;
	private static final GeneratedMethod[] directs;


	private final Class	factoryClass;
	private GCInstanceFactory factory;

	static {
		directs = new GeneratedMethod[10];
		for (int i = 0; i < directs.length; i++) {
			directs[i] = new DirectCall(i);
		}
	}

	public ReflectGeneratedClass(ClassFactory cf, Class jvmClass, Class factoryClass) {
		super(cf, jvmClass);
		methodCache = new Hashtable();
		this.factoryClass = factoryClass;
	}

	public Object newInstance(Context context) throws StandardException	{
		if (factoryClass == null) {
			return super.newInstance(context);
		}

		if (factory == null) {

			Throwable t;
			try {
				factory =  (GCInstanceFactory) factoryClass.newInstance();
				t = null;
			} catch (InstantiationException ie) {
				t = ie;
			} catch (IllegalAccessException iae) {
				t = iae;
			} catch (LinkageError le) {
				t = le;
			}

			if (t != null)
				throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
		}

		GeneratedByteCode ni = factory.getNewInstance();
		ni.initFromContext(context);
		ni.setGC(this);
		ni.postConstructor();
		return ni;

	}

	public GeneratedMethod getMethod(String simpleName)
		throws StandardException {

		GeneratedMethod rm = (GeneratedMethod) methodCache.get(simpleName);
		if (rm != null)
			return rm;

		// Only look for methods that take no arguments
		try {
			if ((simpleName.length() == 2) && simpleName.startsWith("e")) {

				int id = ((int) simpleName.charAt(1)) - '0';

				rm = directs[id];


			}
			else
			{
				Method m = getJVMClass().getMethod(simpleName, (Class []) null);
				
				rm = new ReflectMethod(m);
			}
			methodCache.put(simpleName, rm);
			return rm;

		} catch (NoSuchMethodException nsme) {
			throw StandardException.newException(SQLState.GENERATED_CLASS_NO_SUCH_METHOD,
				nsme, getName(), simpleName);
		}
	}
}

class DirectCall implements GeneratedMethod {

	private final int which;

	DirectCall(int which) {

		this.which = which;
	}

	public Object invoke(Object ref)
		throws StandardException {

		try {

			GeneratedByteCode gref = (GeneratedByteCode) ref;
			switch (which) {
			case 0:
				return gref.e0();
			case 1:
				return gref.e1();
			case 2:
				return gref.e2();
			case 3:
				return gref.e3();
			case 4:
				return gref.e4();
			case 5:
				return gref.e5();
			case 6:
				return gref.e6();
			case 7:
				return gref.e7();
			case 8:
				return gref.e8();
			case 9:
				return gref.e9();
			}
			return null;
		} catch (StandardException se) {
			throw se;
		}		
		catch (Throwable t) {
			throw StandardException.unexpectedUserException(t);
		}
	}
}
