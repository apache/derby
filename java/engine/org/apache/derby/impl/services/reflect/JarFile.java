/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;

import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.services.io.InputStreamUtil;

class JarFile {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	final String[] name;
	protected ZipFile zip;
	boolean isStream;

	JarFile() {
		name = null;
	}

	JarFile(String[] name) {
		this.name = name;
	}

	JarFile newJarFile(String[] name) {
		return new JarFile(name);
	}

	final String getJarName() {
		return IdUtil.mkQualifiedName(name);
	}


	final boolean isZip() {
		return zip != null;
	}

	final ZipFile getZip() {
		return zip;
	}

	void initialize(File jarFile) throws IOException {
		zip = new ZipFile(jarFile);
	}

	final void setInvalid() {
		if (zip != null) {
			try {
				zip.close();
			} catch (IOException ioe) {
			}
			zip = null;

		}
		isStream = false;
	}

	ZipEntry getEntry(String entryName) {
		return zip.getEntry(entryName);
	}
	ZipInputStream getZipOnStream(InputStream in) throws IOException {
		return new ZipInputStream(in);
	}
	ZipEntry getNextEntry(ZipInputStream in) throws IOException {
		return in.getNextEntry();
	}

	byte[] readData(ZipEntry ze, InputStream in, String className) throws IOException {

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
	}

	Object[] getSigners(String className, ZipEntry ze) throws IOException {
		return null;
	}
}
