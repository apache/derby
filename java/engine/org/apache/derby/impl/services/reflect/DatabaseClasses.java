/*

   Derby - Class org.apache.derby.impl.services.reflect.DatabaseClasses

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.compiler.*;
import java.lang.reflect.Modifier;
import org.apache.derby.iapi.sql.compile.CodeGeneration;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.io.FileUtil;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.ClassName;

import java.util.Properties;
import java.util.Hashtable;

import java.io.ObjectStreamClass;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**

    An abstract implementation of the ClassFactory. This package can
	be extended to fully implement a ClassFactory. Implementations can
	differ in two areas, how they load a class and how they invoke methods
	of the generated class.

    <P>
	This class manages a hash table of loaded generated classes and
	their GeneratedClass objects.  A loaded class may be referenced
	multiple times -- each class has a reference count associated
	with it.  When a load request arrives, if the class has already
	been loaded, its ref count is incremented.  For a remove request,
	the ref count is decremented unless it is the last reference,
	in which case the class is removed.  This is transparent to users.

	@see org.apache.derby.iapi.services.loader.ClassFactory
*/

public abstract class DatabaseClasses
	implements ClassFactory, ModuleControl
{
	/*
	** Fields
	*/

	private	ClassInspector	classInspector;
	private JavaFactory		javaFactory;

	private UpdateLoader		applicationLoader;

	/*
	** Constructor
	*/

	public DatabaseClasses() {
	}

	/*
	** Public methods of ModuleControl
	*/

	public void boot(boolean create, Properties startParams)
		throws StandardException
	{

		classInspector = new ClassInspector(this);

		//
		//The ClassFactory runs per service (database) mode (booted as a service module after AccessFactory).
		//If the code that booted
		//us needs a per-database classpath then they pass in the classpath using
		//the runtime property BOOT_DB_CLASSPATH in startParams


		String classpath = null;
		if (startParams != null) {
			classpath = startParams.getProperty(Property.BOOT_DB_CLASSPATH);
		}

		if (classpath != null) {
			applicationLoader = new UpdateLoader(classpath, this, true,
                                                 true);
		}

		javaFactory = (JavaFactory) org.apache.derby.iapi.services.monitor.Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.JavaFactory);
	}



	public void stop() {
		if (applicationLoader != null)
			applicationLoader.close();
	}

	/*
	**	Public methods of ClassFactory
	*/

	/**
		Here we load the newly added class now, rather than waiting for the
		findGeneratedClass(). Thus we are assuming that the class is going
		to be used sometime soon. Delaying the load would mean storing the class
		data in a file, this wastes cycles and compilcates the cleanup.

		@see ClassFactory#loadGeneratedClass

		@exception	StandardException Class format is bad.
	*/
	public final GeneratedClass loadGeneratedClass(String fullyQualifiedName, ByteArray classDump)
		throws StandardException {


			try {


				return loadGeneratedClassFromData(fullyQualifiedName, classDump);

			} catch (LinkageError le) {

			    WriteClassFile(fullyQualifiedName, classDump, le);

				throw StandardException.newException(SQLState.GENERATED_CLASS_LINKAGE_ERROR,
							le, fullyQualifiedName);

    		} catch (VirtualMachineError vme) { // these may be beyond saving, but fwiw

			    WriteClassFile(fullyQualifiedName, classDump, vme);

			    throw vme;
		    }

	}

    private static void WriteClassFile(String fullyQualifiedName, ByteArray bytecode, Throwable t) {

		// get the un-qualified name and add the extension
        int lastDot = fullyQualifiedName.lastIndexOf((int)'.');
        String filename = fullyQualifiedName.substring(lastDot+1,fullyQualifiedName.length()).concat(".class");

		Object env = Monitor.getMonitor().getEnvironment();
		File dir = env instanceof File ? (File) env : null;

		File classFile = FileUtil.newFile(dir,filename);

		// find the error stream
		HeaderPrintWriter errorStream = Monitor.getStream();

		try {
			FileOutputStream fis = new FileOutputStream(classFile);
			fis.write(bytecode.getArray(),
				bytecode.getOffset(), bytecode.getLength());
			fis.flush();
			if (t!=null) {				
				errorStream.printlnWithHeader(MessageService.getTextMessage(MessageId.CM_WROTE_CLASS_FILE, fullyQualifiedName, classFile, t));
			}
			fis.close();
		} catch (IOException e) {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unable to write .class file");
		}
	}

	public ClassInspector getClassInspector() {
		return classInspector;
	}


	public final Class loadApplicationClass(String className)
		throws ClassNotFoundException {

		try {
			return loadClassNotInDatabaseJar(className);
		} catch (ClassNotFoundException cnfe) {
			if (applicationLoader == null)
				throw cnfe;
			Class c = applicationLoader.loadClass(className, true);
			if (c == null)
				throw cnfe;
			return c;
		}
	}

	protected Class loadClassNotInDatabaseJar(String className) throws ClassNotFoundException {
		return Class.forName(className);
	}


	public final Class loadApplicationClass(ObjectStreamClass classDescriptor)
		throws ClassNotFoundException {
		return loadApplicationClass(classDescriptor.getName());
	}

	public boolean isApplicationClass(Class theClass) {

		return theClass.getClassLoader()
			instanceof JarLoader;
	}

	public void notifyModifyJar(boolean reload) throws StandardException  {
		if (applicationLoader != null) {
			applicationLoader.modifyJar(reload);
		}
	}

	/**
		Notify the class manager that the classpath has been modified.

		@exception StandardException thrown on error
	*/
	public void notifyModifyClasspath(String classpath) throws StandardException {

		if (applicationLoader != null) {
			applicationLoader.modifyClasspath(classpath);
		}
	}


	public int getClassLoaderVersion() {
		if (applicationLoader != null) {
			return applicationLoader.getClassLoaderVersion();
		}

		return -1;
	}

	public ByteArray buildSpecificFactory(String className, String factoryName) {

		ClassBuilder cb = javaFactory.newClassBuilder(this, CodeGeneration.GENERATED_PACKAGE_PREFIX,
			Modifier.PUBLIC | Modifier.FINAL, factoryName, "org.apache.derby.impl.services.reflect.GCInstanceFactory");

		MethodBuilder constructor = cb.newConstructorBuilder(Modifier.PUBLIC);

		constructor.callSuper();
		constructor.methodReturn();
		constructor.complete();
		constructor = null;

		MethodBuilder noArg = cb.newMethodBuilder(Modifier.PUBLIC, ClassName.GeneratedByteCode, "getNewInstance");
		noArg.pushNewStart(className);
		noArg.pushNewComplete(0);
		noArg.methodReturn();
		noArg.complete();
		noArg = null;

		return cb.getClassBytecode();
	}

	/*
	** Class specific methods
	*/
	
	/*
	** Keep track of loaded generated classes and their GeneratedClass objects.
	*/

	protected abstract LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump); 
}
