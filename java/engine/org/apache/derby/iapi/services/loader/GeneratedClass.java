/*

   Derby - Class org.apache.derby.iapi.services.loader.GeneratedClass

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

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.Context;

/**
	A meta-class that represents a generated class.
	(Similar to java.lang.Class).
*/

public interface GeneratedClass {

	/**
		Return the name of the generated class.
	*/
	public String getName();

	/**
		Return a new object that is an instance of the represented
		class. The object will have been initialised by the no-arg
		constructor of the represneted class.
		(Similar to java.lang.Class.newInstance).

		@exception 	StandardException	Standard Cloudscape error policy

	*/
	public Object newInstance(Context context)
		throws StandardException;

	/**
		Obtain a handle to the method with the given name
		that takes no arguments.

		@exception 	StandardException	Standard Cloudscape error policy
	*/
	public GeneratedMethod getMethod(String simpleName)
		throws StandardException;

	/**
		Return the class reload version that this class was built at.
	*/
	public int getClassLoaderVersion();
}

