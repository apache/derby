/*

   Derby - Class org.apache.derby.impl.services.reflect.JarLoader

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

import org.apache.derby.impl.sql.execute.JarUtil;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.error.StandardException;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;

import java.sql.*;
import org.apache.derby.iapi.services.io.LimitInputStream;
import org.apache.derby.iapi.util.IdUtil;

import org.apache.derby.iapi.services.info.JVMInfo;

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.services.i18n.MessageService;


public class JarLoader extends ClassLoader {

	private static final JarFile jarFileFactory;

	static {
		JarFile jf = null;

		if (JVMInfo.JDK_ID >= 2) {
			try {
				Class jf2c = Class.forName("org.apache.derby.impl.services.reflect.JarFileJava2");
				jf = (JarFile) jf2c.newInstance();
			} catch (Exception e) {
				throw new ExceptionInInitializerError(e);
			}
		} else {	
			jf = new JarFile();
		}

		jarFileFactory = jf;
	}

	private UpdateLoader updateLoader;
	private JarFile jf;
	private HeaderPrintWriter vs;

	JarLoader(UpdateLoader updateLoader, String[] name, HeaderPrintWriter vs) {

		this.updateLoader = updateLoader;
		this.jf = jarFileFactory.newJarFile(name);
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
		setInvalid(false);	
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
			if (jf.isZip())
				return loadClassDataFromJar(className, jvmClassName, resolve);

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

		if (jf.isZip())
			return getRawStream(jf.getZip(), name);

		if (jf.isStream) {
			return getRawStream((InputStream) load(), name);
		}
		return null;
	}


	/*
	** Private api
	*/


	private Class loadClassDataFromJar(String className, String jvmClassName, boolean resolve) 
		throws IOException {

		ZipEntry ze = jf.getEntry(jvmClassName);
		if (ze == null)
			return null;

		InputStream in = jf.getZip().getInputStream(ze);

		try {
			return loadClassData(ze, in, className, resolve);
		} finally {
			in.close();
		}
	}

	private Class loadClassData(
		InputStream in, String className, String jvmClassName, boolean resolve) 
		throws IOException {

		ZipInputStream zipIn = jf.getZipOnStream(in);

		for (;;) {

			ZipEntry ze = jf.getNextEntry(zipIn);
			if (ze == null) {
				zipIn.close();
				return null;
			}

			if (ze.getName().equals(jvmClassName)) {
				Class c = loadClassData(ze, zipIn, className, resolve);
				zipIn.close();
				return c;
			}
		}
		
	}

	private Class loadClassData(ZipEntry ze, InputStream in,
		String className, boolean resolve) throws IOException {

		byte[] data = jf.readData(ze, in, className);

		Object[] signers = jf.getSigners(className, ze);

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

	JarFile setInvalid(boolean newJarFile) {

		jf.setInvalid();
		updateLoader = null;
		return newJarFile ? jarFileFactory.newJarFile(jf.name) : null;
	}

	/*
	** Routines to get an InputStream for a namedResource
	*/

	/**
		Get a stream directly from a ZipFile.
		In this case we can safely return the stream directly.
		It's a new stream set up by the zip code to read just
		the contents of this entry.
	*/
	private InputStream getRawStream(ZipFile zip, String name) {

		try {
			ZipEntry ze = zip.getEntry(name);
			if (ze == null)
				return null;

			return zip.getInputStream(ze);
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

		ZipInputStream zipIn = null;
		try {
			zipIn = new ZipInputStream(in);

			ZipEntry ze;
			while ((ze = jf.getNextEntry(zipIn)) != null) {

				if (ze.getName().equals(name)) {
					LimitInputStream lis = new LimitInputStream(zipIn);
					lis.setLimit((int) ze.getSize());
					return lis;
				}
			}

			zipIn.close();

		} catch (IOException ioe) {
			if (zipIn != null) {
				try {
					zipIn.close();
				} catch (IOException ioe2) {
				}
			}
		}
		return null;
	}
}
