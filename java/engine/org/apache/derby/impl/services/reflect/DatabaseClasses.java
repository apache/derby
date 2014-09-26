/*

   Derby - Class org.apache.derby.impl.services.reflect.DatabaseClasses

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectStreamClass;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.util.ByteArray;

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

abstract class DatabaseClasses
	implements ClassFactory, ModuleControl
{
	/*
	** Fields
	*/

	private	ClassInspector	classInspector;

	private UpdateLoader		applicationLoader;

	/*
	** Constructor
	*/

	DatabaseClasses() {
	}

	/*
	** Public methods of ModuleControl
	*/

	public void boot(boolean create, Properties startParams)
		throws StandardException
	{

		classInspector = makeClassInspector( this );

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
	}



	public void stop() {
		if (applicationLoader != null)
			applicationLoader.close();
	}

    /**
     * For creating the class inspector.
     */
    protected   ClassInspector  makeClassInspector( DatabaseClasses dc )
    {
        return new ClassInspector( dc );
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

		Object env = getMonitor().getEnvironment();
		File dir = env instanceof File ? (File) env : null;

        final File classFile = new File(dir, filename);

		// find the error stream
		HeaderPrintWriter errorStream = Monitor.getStream();

		try {
            FileOutputStream fis;
            try {
                fis = AccessController.doPrivileged(
                        new PrivilegedExceptionAction<FileOutputStream>() {
                            public FileOutputStream run() throws IOException {
                                return new FileOutputStream(classFile);
                            }
                        });
            } catch (PrivilegedActionException pae) {
                throw (IOException) pae.getCause();
            }
			fis.write(bytecode.getArray(),
				bytecode.getOffset(), bytecode.getLength());
			fis.flush();
			if (t!=null) {				
				errorStream.printlnWithHeader(MessageService.getTextMessage(MessageId.CM_WROTE_CLASS_FILE, fullyQualifiedName, classFile, t));
			}
			fis.close();
		} catch (IOException e) {
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Unable to write .class file", e);
		}
	}

	public ClassInspector getClassInspector() {
		return classInspector;
	}


	public final Class loadApplicationClass(String className)
		throws ClassNotFoundException {
        
        if (className.startsWith("org.apache.derby.")) {
            // Assume this is an engine class, if so
            // try to load from this class loader,
            // this ensures in strange class loader
            // environments we do not get ClassCastExceptions
            // when an engine class is loaded through a different
            // class loader to the rest of the engine.
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException cnfe)
            {
                // fall through to the code below,
                // could be client or tools class
                // in a different loader.
            }
        }
 
		Throwable loadError;
		try {
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
		catch (SecurityException se)
		{
			// Thrown if the class has been comprimised in some
			// way, e.g. modified in a signed jar.
			loadError = se;	
		}
		catch (LinkageError le)
		{
			// some error linking the jar, again could
			// be malicious code inserted into a jar.
			loadError = le;	
		}
		throw new ClassNotFoundException(className + " : " + loadError.getMessage());
	}
	
	abstract Class loadClassNotInDatabaseJar(String className)
		throws ClassNotFoundException;

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

	/*
	** Class specific methods
	*/
	
	/*
	** Keep track of loaded generated classes and their GeneratedClass objects.
	*/

	abstract LoadedGeneratedClass loadGeneratedClassFromData(String fullyQualifiedName, ByteArray classDump); 
    
    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }

}
