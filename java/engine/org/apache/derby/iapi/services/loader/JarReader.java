/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.error.StandardException;

/**
	Abstract out the loading of JarFiles.
*/

public interface JarReader {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	/**
		Load the contents of a Jarfile. The return is either
		an java.io.InputStream representing the contents of the JarFile
		or a java.io.File representing the location of the file.
		If the jar does not exist an exception is thrown.
	*/
	Object readJarFile(String schemaName, String sqlName)
		throws StandardException;
}

