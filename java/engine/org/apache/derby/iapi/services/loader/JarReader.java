/*

   Derby - Class org.apache.derby.iapi.services.loader.JarReader

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

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.error.StandardException;

/**
	Abstract out the loading of JarFiles.
*/

public interface JarReader {

	/**
		Load the contents of a Jarfile. The return is either
		an java.io.InputStream representing the contents of the JarFile
		or a java.io.File representing the location of the file.
		If the jar does not exist an exception is thrown.
	*/
	Object readJarFile(String schemaName, String sqlName)
		throws StandardException;
}

