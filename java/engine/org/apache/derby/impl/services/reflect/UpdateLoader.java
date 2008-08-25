/*

   Derby - Class org.apache.derby.impl.services.reflect.UpdateLoader

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

import org.apache.derby.iapi.services.context.ContextService;
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
import java.security.AccessController;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Module;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.locks.LockOwner;

/**
 * UpdateLoader implements then functionality of
 * derby.database.classpath. It manages the ClassLoaders
 * (instances of JarLoader) for each installed jar file.
 * Jar files are installed through the sqlj.install_jar procedure.
 * <BR>
 * Each JarLoader delegates any request through standard mechanisms
 * to load a class to this object, which will then ask each jarLoader in order of
 * derby.database.classpath to load the class through an internal api.
 * This means if the third jar in derby.database.classpath tries to load
 * a class, say from the class for a procedure's method making some
 * reference to it, then the request is delegated to UpdateLoader.
 * UpdateLoader will then try to load the class from each of the jars
 * in order of derby.database.classpath using the jar's installed JarLoader.
 */
final class UpdateLoader implements LockOwner {
    
    /**
     * List of packages that Derby will not support being loaded
     * from an installed jar file.
     */
    private static final String[] RESTRICTED_PACKAGES = {
        // While loading java. classes is blocked by the standard
        // class loading mechanism, javax. ones are not. However
        // allowing database applications to override jvm classes
        // seems a bad idea.
        "javax.",
        
        // Allowing an application to possible override the engine's
        // own classes also seems dangerous.
        "org.apache.derby.",
    };

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
	private final CompatibilitySpace compat;

	private boolean needReload;
	private JarReader jarReader;

	UpdateLoader(String classpath, DatabaseClasses parent, boolean verbose, boolean normalizeToUpper) 
		throws StandardException {

        this.normalizeToUpper = normalizeToUpper;
		this.parent = parent;
		lf = (LockFactory) Monitor.getServiceModule(parent, Module.LockFactory);
		compat = lf.createCompatibilitySpace(this);

		if (verbose) {
			vs = Monitor.getStream();
		}
		
		myLoader = getClass().getClassLoader();

		this.classLoaderLock = new ClassLoaderLock(this);

		initializeFromClassPath(classpath);
	}

	private void initializeFromClassPath(String classpath) throws StandardException {

		final String[][] elements = IdUtil.parseDbClassPath(classpath);
		
		final int jarCount = elements.length;
		jarList = new JarLoader[jarCount];
			
        if (jarCount != 0) {
            // Creating class loaders is a restricted operation
            // so we need to use a privileged block.
            AccessController.doPrivileged
            (new java.security.PrivilegedAction(){
                
                public Object run(){    
    		      for (int i = 0; i < jarCount; i++) {
    			     jarList[i] = new JarLoader(UpdateLoader.this, elements[i], vs);
    		      }
                  return null;
                }
            });
        }
		if (vs != null) {
			vs.println(MessageService.getTextMessage(MessageId.CM_CLASS_LOADER_START, classpath));
		}
		
		thisClasspath = classpath;
		initDone = false;
	}

	/**
		Load the class from the class path. Called by JarLoader
        when it has a request to load a class to fulfill
        the sematics of derby.database.classpath.
        <P>
        Enforces two restrictions:
        <UL>
        <LI> Do not allow classes in certain name spaces to be loaded
        from installed jars, see RESTRICTED_PACKAGES for the list.
        <LI> Referencing Derby's internal classes (those outside the
        public api) from installed is disallowed. This is to stop
        user defined routines bypassing security or taking advantage
        of security holes in Derby. E.g. allowing a routine to
        call a public method in derby would allow such routines
        to call public static methods for system procedures without
        having been granted permission on them, such as setting database
        properties.
        </UL>

		@exception ClassNotFoundException Class can not be found or
        the installed jar is restricted from loading it.
	*/
	Class loadClass(String className, boolean resolve) 
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
                
                // Refuse to load classes from restricted name spaces
                // That is classes in those name spaces can be not
                // loaded from installed jar files.
                for (int i = 0; i < RESTRICTED_PACKAGES.length; i++)
                {
                    if (className.startsWith(RESTRICTED_PACKAGES[i]))
                        throw new ClassNotFoundException(className);
                }

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
				lf.unlock(compat, this, classLoaderLock, ShExQual.SH);
			}
		}
	}

	InputStream getResourceAsStream(String name) {

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
				lf.unlock(compat, this, classLoaderLock, ShExQual.SH);
			}
		}
	}

	synchronized void modifyClasspath(String classpath)
		throws StandardException {

		// lock transaction classloader exclusively
		lockClassLoader(ShExQual.EX);
		version++;


		modifyJar(false);
		initializeFromClassPath(classpath);
	}


	synchronized void modifyJar(boolean reload) throws StandardException {

		// lock transaction classloader exclusively
		lockClassLoader(ShExQual.EX);
		version++;

		if (!initDone)
			return;
        
        // first close the existing jar file opens
        close();

		if (reload) {
			initializeFromClassPath(thisClasspath);
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
		CompatibilitySpace lockSpace = null;
		
		if (cfc != null) {
			lockSpace = cfc.getLockSpace();
		}
		if (lockSpace == null)
			lockSpace = compat;

		Object lockGroup = lockSpace.getOwner();

		lf.lockObject(lockSpace, lockGroup, classLoaderLock, qualifier,
					  C_LockFactory.TIMED_WAIT);

		return (lockGroup == this);
	}

	Class checkLoaded(String className, boolean resolve) {

		for (int i = 0; i < jarList.length; i++) {
			Class c = jarList[i].checkLoaded(className, resolve);
			if (c != null)
				return c;
		}
		return null;
	}

	void close() {

		for (int i = 0; i < jarList.length; i++) {
			jarList[i].setInvalid();
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

	int getClassLoaderVersion() {
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

    /**
     * Tell the lock manager that we don't want timed waits to time out
     * immediately.
     *
     * @return {@code false}
     */
    public boolean noWait() {
        return false;
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
