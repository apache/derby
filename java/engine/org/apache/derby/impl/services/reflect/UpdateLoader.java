/*

   Derby - Class org.apache.derby.impl.services.reflect.UpdateLoader

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

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.locks.ShExLockable;
import org.apache.derby.iapi.services.locks.ShExQual;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.locks.Latch;
import org.apache.derby.iapi.services.locks.C_LockFactory;
import org.apache.derby.iapi.services.loader.ClassFactoryContext;
import org.apache.derby.iapi.services.loader.JarReader;
import org.apache.derby.iapi.services.property.PersistentSet;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.reference.Property;

import java.io.InputStream;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Module;
import org.apache.derby.iapi.services.i18n.MessageService;

public class UpdateLoader {

	private JarLoader[] jarList;
	private HeaderPrintWriter vs;
	private final ClassLoader myLoader;
	private boolean initDone;
	private String thisClasspath;
	private final LockFactory lf;
	private final ShExLockable classLoaderLock;
	private int version;
    private boolean normalizeToUpper;
	private DatabaseClasses parent;

	private boolean needReload;
	private JarReader jarReader;

	public UpdateLoader(String classpath, DatabaseClasses parent, boolean verbose, boolean normalizeToUpper) 
		throws StandardException {

        this.normalizeToUpper = normalizeToUpper;
		this.parent = parent;
		lf = (LockFactory) Monitor.getServiceModule(parent, Module.LockFactory);

		if (verbose) {
			vs = Monitor.getStream();
		}
		
		myLoader = getClass().getClassLoader();

		this.classLoaderLock = new ClassLoaderLock(this);

		initializeFromClassPath(classpath);
	}

	private void initializeFromClassPath(String classpath) throws StandardException {

		String[][] elements = IdUtil.parseDbClassPath(classpath, normalizeToUpper);
		
		int jarCount = elements.length;
		jarList = new JarLoader[jarCount];
			
		for (int i = 0; i < jarCount; i++) {
			jarList[i] = new JarLoader(this, elements[i], vs);
		}

		if (vs != null) {
			vs.println(MessageService.getTextMessage(MessageId.CM_CLASS_LOADER_START, classpath));
		}
		
		thisClasspath = classpath;
		initDone = false;
	}

	/**
		Load the class from the class path.

		@exception ClassNotFoundException Class can not be found
	*/
	public Class loadClass(String className, boolean resolve) 
		throws ClassNotFoundException {


		JarLoader jl = null;

		boolean unlockLoader = false;
		try {
			unlockLoader = lockClassLoader(ShExQual.SH);

			synchronized (this) {

				if (needReload) {
					reload();
				}
			
				Class clazz = checkLoaded(className, resolve);
				if (clazz != null)
					return clazz;

				String jvmClassName = className.replace('.', '/').concat(".class");

				if (!initDone)
					initLoaders();

				for (int i = 0; i < jarList.length; i++) {

					jl = jarList[i];

					Class c = jl.loadClassData(className, jvmClassName, resolve);
					if (c != null) {
						if (vs != null)
							vs.println(MessageService.getTextMessage(MessageId.CM_CLASS_LOAD, className, jl.getJarName()));

						return c;
					}
				}
			}

			return null;


		} catch (StandardException se) {
			throw new ClassNotFoundException(MessageService.getTextMessage(MessageId.CM_CLASS_LOAD_EXCEPTION, className, jl == null ? null : jl.getJarName(), se));
		} finally {
			if (unlockLoader) {
				lf.unlock(this, this, classLoaderLock, ShExQual.SH);
			}
		}
	}

	public InputStream getResourceAsStream(String name) {

		InputStream is = (myLoader == null) ?
			ClassLoader.getSystemResourceAsStream(name) :
			myLoader.getResourceAsStream(name);

		if (is != null)
			return is;

		// match behaviour of standard class loaders. 
		if (name.endsWith(".class"))
			return null;

		boolean unlockLoader = false;
		try {
			unlockLoader = lockClassLoader(ShExQual.SH);

			synchronized (this) {

				if (needReload) {
					reload();		
				}

				if (!initDone)
					initLoaders();

				for (int i = 0; i < jarList.length; i++) {

					JarLoader jl = jarList[i];

					is = jl.getStream(name);
					if (is != null) {
						return is;
					}
				}
			}
			return null;

		} catch (StandardException se) {
			return null;
		} finally {
			if (unlockLoader) {
				lf.unlock(this, this, classLoaderLock, ShExQual.SH);
			}
		}
	}

	public synchronized void modifyClasspath(String classpath)
		throws StandardException {

		// lock transaction classloader exclusively
		lockClassLoader(ShExQual.EX);
		version++;


		modifyJar(false);
		initializeFromClassPath(classpath);
	}


	public synchronized void modifyJar(boolean reload) throws StandardException {

		// lock transaction classloader exclusively
		lockClassLoader(ShExQual.EX);
		version++;

		if (!initDone)
			return;

		if (reload) {
			//first close the existing jar file opens
			close();
			initializeFromClassPath(thisClasspath);
			return;
		}

		// first thing to do is to remove all Class entries
		// and then get a complete set of loaders ...
		synchronized (this) {

			for (int i = 0; i < jarList.length; i++) {

				JarLoader jl = jarList[i];

				JarFile newJarFile = jl.setInvalid(reload);
			}
		}
	}

	private boolean lockClassLoader(ShExQual qualifier)
		throws StandardException {

		if (lf == null)
			return false;

		ClassFactoryContext cfc = (ClassFactoryContext) ContextService.getContextOrNull(ClassFactoryContext.CONTEXT_ID);

		// This method can be called from outside of the database
		// engine, in which case tc will be null. In that case
		// we lock the class loader only for the duration of
		// the loadClass().
		Object lockSpace = null;
		
		if (cfc != null) {
			lockSpace = cfc.getLockSpace();
		}
		if (lockSpace == null)
			lockSpace = this;

		lf.lockObject(lockSpace, lockSpace, classLoaderLock, qualifier, C_LockFactory.TIMED_WAIT);

		return (lockSpace == this);
	}

	Class checkLoaded(String className, boolean resolve) {

		for (int i = 0; i < jarList.length; i++) {
			Class c = jarList[i].checkLoaded(className, resolve);
			if (c != null)
				return c;
		}
		return null;
	}

	public void close() {

		for (int i = 0; i < jarList.length; i++) {
			jarList[i].setInvalid(false);
		}

	}

	private void initLoaders() {

		if (initDone)
			return;

		for (int i = 0; i < jarList.length; i++) {
			jarList[i].initialize();
		}
		initDone = true;
	}

	public int getClassLoaderVersion() {
		return version;
	}

	synchronized void needReload() {
		version++;
		needReload = true;
	}

	private void reload() throws StandardException {
		thisClasspath = getClasspath();
		// first close the existing jar file opens
		close();
		initializeFromClassPath(thisClasspath);
		needReload = false;
	}


	private String getClasspath()
		throws StandardException {

		ClassFactoryContext cfc = (ClassFactoryContext) ContextService.getContextOrNull(ClassFactoryContext.CONTEXT_ID);

		PersistentSet ps = cfc.getPersistentSet();
		
		String classpath = PropertyUtil.getServiceProperty(ps, Property.DATABASE_CLASSPATH);

		//
		//In per database mode we must always have a classpath. If we do not
		//yet have one we make one up.
		if (classpath==null)
			classpath="";


		return classpath;
	}

	JarReader getJarReader() {
		if (jarReader == null) {

			ClassFactoryContext cfc = (ClassFactoryContext) ContextService.getContextOrNull(ClassFactoryContext.CONTEXT_ID);

			jarReader = cfc.getJarReader(); 
		}
		return jarReader;
	}
}


class ClassLoaderLock extends ShExLockable {

	private UpdateLoader myLoader;

	ClassLoaderLock(UpdateLoader myLoader) {
		this.myLoader = myLoader;
	}

	public void unlockEvent(Latch lockInfo)
	{
		super.unlockEvent(lockInfo);

		if (lockInfo.getQualifier().equals(ShExQual.EX)) {
			// how do we tell if we are reverting or not
			myLoader.needReload();
		}
	}
}
