/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
