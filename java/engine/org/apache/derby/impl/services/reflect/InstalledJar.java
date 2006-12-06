/*

   Derby - Class org.apache.derby.impl.services.reflect.InstalledJar

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

import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.services.io.InputStreamUtil;

/**
 * Represents a jar file for class loading,
 * previously installed by the sqlj.install_jar or replace_jar procedures.
 * <br>
 * The source for the Jar is either a File (database from a file system)
 * or an InputStream (database is in a jar file itself).
 */
final class InstalledJar {
    /**
     * Two part name for the jar file.
     */
    final String[] name;
    
    /**
     * When the jar file can be manipulated as a java.util.JarFile
     * this holds the reference to the open jar. When the jar can
     * only be manipulated as an InputStream (because the jar is itself
     * in a database jar) then this will be null.
     */
    private JarFile jar;
    
    /**
     * True if the jar can only be accessed using a stream, because
     * the jar is itself in a database jar. When fals the jar is accessed
     * using the jar field.
     */
    boolean isStream;

	InstalledJar(String[] name) {
		this.name = name;
	}

    /**
     * Return the SQL name for the installed jar.
     * Used for error and informational messages.
     */
	final String getJarName() {
		return IdUtil.mkQualifiedName(name);
	}

    /**
     * Return the JarFile for this installed jar when it
     * is being accessed through java.util.jar.
     */
	final JarFile getJarFile() {
		return jar;
	}

	void initialize(File jarFile) throws IOException {
        jar = new JarFile(jarFile);
	}

	final void setInvalid() {
		if (jar != null) {
			try {
				jar.close();
			} catch (IOException ioe) {
			}
			jar = null;

		}
		isStream = false;
	}

	byte[] readData(JarEntry ze, InputStream in, String className) throws IOException {

		try {
            int size = (int) ze.getSize();

            if (size != -1) {
            	byte[] data = new byte[size];

            	InputStreamUtil.readFully(in, data, 0, size);

            	return data;
            }

            // unknown size
            byte[] data = new byte[1024];
            ByteArrayOutputStream os = new ByteArrayOutputStream(1024);
            int r;
            while ((r = in.read(data)) != -1) {
            	os.write(data, 0, r);
            }

            data = os.toByteArray();
            return data;
        } catch (SecurityException se) {
            throw handleException(se, className);
        }
	}

    Object[] getSigners(String className, JarEntry je) throws IOException {
        Exception e;

        try {
            Certificate[] list = je.getCertificates();
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