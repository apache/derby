/*

   Derby - Class org.apache.derby.iapi.sql.compile.TypeCompilerFactory

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.types.TypeId;

/**
 * Factory interface for the compilation part of datatypes.
 *
 * @author Jeff Lichtman
 */

public interface TypeCompilerFactory
{
	public static final String MODULE = "org.apache.derby.iapi.sql.compile.TypeCompilerFactory";

	/**
	 * Get a TypeCompiler corresponding to the given TypeId.
	 *
	 * @return	A TypeCompiler
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TypeCompiler getTypeCompiler(TypeId typeId);
}

