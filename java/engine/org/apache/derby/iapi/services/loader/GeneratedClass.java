/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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

