/*

   Derby - Class org.apache.derby.impl.services.reflect.JarLoader

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

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.error.StandardException;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.util.IdUtil;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.services.i18n.MessageService;


class JarLoader extends ClassLoader {

	private UpdateLoader updateLoader;
	private InstalledJar jf;
	private HeaderPrintWriter vs;

	JarLoader(UpdateLoader updateLoader, String[] name, HeaderPrintWriter vs) {

		this.updateLoader = updateLoader;
		this.jf = new InstalledJar(name);
		this.vs = vs;
	}

	// Initialize the class loader so it knows if it
	// is loading from a ZipFile or an InputStream
	void initialize() {

		Object zipData = load();

		try {

			if (zipData instanceof File) {
				jf.initialize((File) zipData);
				return;
			}

			if (zipData instanceof InputStream) {
				jf.isStream = true;
				try {
					((InputStream) zipData).close();
				} catch (IOException ioe) {
				}
				return;
			}
		} catch (IOException ioe) {
			if (vs != null)
				vs.println(MessageService.getTextMessage(MessageId.CM_LOAD_JAR_EXCEPTION, getJarName(), ioe));
		}

		// No such zip.
		setInvalid();	
	}

	/**
		Handle all requests to the top-level loader.

		@exception ClassNotFoundException Class can not be found
	*/
	public Class loadClass(String className, boolean resolve) 
		throws ClassNotFoundException {

		// we attempt the system class load even if we
		// are stale because otherwise we will fail
		// to load java.* classes which confuses some VMs
		try {
			return Class.forName(className);
		} catch (ClassNotFoundException cnfe) {

			if (updateLoader == null)
				throw new ClassNotFoundException(MessageService.getTextMessage(MessageId.CM_STALE_LOADER, className));

			Class c = updateLoader.loadClass(className, resolve);
			if (c == null)
				throw cnfe;
			return c;
		}
	}

	/**
		
	*/
	public InputStream getResourceAsStream(String name) {
		if (updateLoader == null)
			return null;
		return updateLoader.getResourceAsStream(name);
	}

	/*
	** Package level api
	*/
	final String getJarName() {
		return jf.getJarName();
	}

	Class loadClassData(String className, String jvmClassName, boolean resolve) {

		if (updateLoader == null)
			return null;

		try {
            JarFile jar = jf.getJarFile();
			if (jar != null)
				return loadClassDataFromJar(jar, className, jvmClassName, resolve);

			if (jf.isStream) {
				// have to use a new stream each time
				return loadClassData((InputStream) load(),
						className, jvmClassName, resolve);
			}

			return null;
		} catch (IOException ioe) {
			if (vs != null)
				vs.println(MessageService.getTextMessage(MessageId.CM_CLASS_LOAD_EXCEPTION, className, getJarName(), ioe));
			return null;
		}	
	}

	/**
		Get an InputStream for the given resource.
	*/
	InputStream getStream(String name) {

		if (updateLoader == null)
			return null;
        
        JarFile jar = jf.getJarFile();

		if (jar != null)
			return getRawStream(jar, name);

		if (jf.isStream) {
			return getRawStream((InputStream) load(), name);
		}
		return null;
	}


	/*
	** Private api
	*/


    /**
     * Load the class data when the installed jar is accessible
     * as a java.util.jarFile.
     */
	private Class loadClassDataFromJar(
            JarFile jar,
            String className, String jvmClassName, boolean resolve) 
		throws IOException {

		JarEntry e = jar.getJarEntry(jvmClassName);
		if (e == null)
			return null;

		InputStream in = jar.getInputStream(e);

		try {
			return loadClassData(e, in, className, resolve);
		} finally {
			in.close();
		}
	}

    /**
     * Load the class data when the installed jar is accessible
     * only as an input stream (the jar is itself in a database jar).
     */
	private Class loadClassData(
		InputStream in, String className, String jvmClassName, boolean resolve) 
		throws IOException {

        JarInputStream jarIn = new JarInputStream(in);

		for (;;) {

			JarEntry e = jarIn.getNextJarEntry();
			if (e == null) {
				jarIn.close();
				return null;
			}

			if (e.getName().equals(jvmClassName)) {
				Class c = loadClassData(e, jarIn, className, resolve);
				jarIn.close();
				return c;
			}
		}
		
	}

	private Class loadClassData(JarEntry e, InputStream in,
		String className, boolean resolve) throws IOException {

		byte[] data = jf.readData(e, in, className);

		Object[] signers = jf.getSigners(className, e);

		synchronized (updateLoader) {
			// see if someone else loaded it while we
			// were getting the bytes ...
			Class c = updateLoader.checkLoaded(className, resolve);
			if (c == null) {
				c = defineClass(className, data, 0, data.length);
				if (signers != null) {
					setSigners(c, signers);
				}
				if (resolve)
					resolveClass(c);
			}
			return c;

		}
	}

	Class checkLoaded(String className, boolean resolve) {
		if (updateLoader == null)
			return null;

		Class c = findLoadedClass(className);
		if ((c != null) && resolve)
			resolveClass(c);
		return c;
	}

	private Object load() {

		String[] dbJarName = jf.name;

		String schemaName = dbJarName[IdUtil.DBCP_SCHEMA_NAME];
		String sqlName = dbJarName[IdUtil.DBCP_SQL_JAR_NAME];

		// don't need a connection, just call the code directly
		try {
			return updateLoader.getJarReader().readJarFile(schemaName, sqlName);
		} catch (StandardException se) {
			if (vs != null)
				vs.println(MessageService.getTextMessage(MessageId.CM_LOAD_JAR_EXCEPTION, jf.getJarName(), se));
			return null;
		}

	}

    /**
     * Set this loader to be invaid so that it will not
     * resolve any classes or resources.
     *
     */
	void setInvalid() {

		jf.setInvalid();
		updateLoader = null;
	}

	/*
	** Routines to get an InputStream for a namedResource
	*/

	/**
		Get a stream for a resource directly from a JarFile.
		In this case we can safely return the stream directly.
		It's a new stream set up by the zip code to read just
		the contents of this entry.
	*/
	private InputStream getRawStream(JarFile jar, String name) {

		try {
			JarEntry e = jar.getJarEntry(name);
			if (e == null)
				return null;

			return jar.getInputStream(e);
		} catch (IOException ioe) {
			return null;
		}
	}

	/**
		Get a stream from a zip file that is itself a stream.
		Here we need to get the size of the zip entry and
		put a limiting stream around it. Otherwise the
		caller would end up reading the entire zip file!
	*/
	private InputStream getRawStream(InputStream in, String name) { 

		JarInputStream jarIn = null;
		try {
			jarIn = new JarInputStream(in);

		    JarEntry e;
			while ((e = jarIn.getNextJarEntry()) != null) {

				if (e.getName().equals(name)) {
					LimitInputStream lis = new LimitInputStream(jarIn);
					lis.setLimit((int) e.getSize());
					return lis;
				}
			}

			jarIn.close();

		} catch (IOException ioe) {
			if (jarIn != null) {
				try {
					jarIn.close();
				} catch (IOException ioe2) {
				}
			}
		}
		return null;
	}
}
