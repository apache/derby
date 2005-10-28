/*

   Derby - Class org.apache.derby.impl.services.reflect.JarFileJava2

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;

// below are all Java2 imports.
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.GeneralSecurityException;



/**
	Sub-class of JarFile for a Java2 environment that uses the
	java.util.jar.* classes to be signature aware.
*/

final class JarFileJava2 extends JarFile {

	JarFileJava2() {
		super();
	}

	JarFileJava2(String[] name) {
		super(name);
	}

	JarFile newJarFile(String[] name) {
		return new JarFileJava2(name);
	}

	void initialize(File jarFile) throws IOException {

		java.util.jar.JarFile jf = new java.util.jar.JarFile(jarFile);

		// determine if it is signed.
		zip = jf;
	}

	ZipEntry getEntry(String entryName) {
		return ((java.util.jar.JarFile) zip).getJarEntry(entryName);
	}
	ZipInputStream getZipOnStream(InputStream in) throws IOException {
		return new java.util.jar.JarInputStream(in);
	}
	ZipEntry getNextEntry(ZipInputStream in) throws IOException {
		return ((java.util.jar.JarInputStream) in).getNextJarEntry();
	}

	byte[] readData(ZipEntry ze, InputStream in, String className) throws IOException {
		try {
			return super.readData(ze, in, className);
		} catch (SecurityException se) {
			throw handleException(se, className);
		}
	}

	Object[] getSigners(String className, ZipEntry ze) throws IOException {
		Exception e;

		try {
			Certificate[] list = ((java.util.jar.JarEntry) ze).getCertificates();
			if ((list == null) || (list.length == 0)) {
				return null;
			}

			for (int i = 0; i < list.length; i++) {
				if (!(list[i] instanceof X509Certificate)) {
					String msg = MessageService.getTextMessage(MessageId.CM_UNKNOWN_CERTIFICATE, className, getJarName());

					throw new SecurityException(msg);
				}

				X509Certificate cert = (X509Certificate) list[i];

				cert.checkValidity();
			}

			return list;

		} catch (GeneralSecurityException gse) {
			// convert this into an unchecked security
			// exception. Unchecked as eventually it has
			// to pass through a method that's only throwing
			// ClassNotFoundException
			e = gse;
		}
		throw handleException(e, className);
	}

	private SecurityException handleException(Exception e, String className) {
		String msg = MessageService.getTextMessage(MessageId.CM_SECURITY_EXCEPTION, className, getJarName(), e.getLocalizedMessage());
		return new SecurityException(msg);
	}
}
