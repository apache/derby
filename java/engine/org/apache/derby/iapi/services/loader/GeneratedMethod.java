/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.error.StandardException;

/**
	Handle for a method within a generated class.

	@see GeneratedClass
*/

public interface GeneratedMethod { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;


	/**
		Invoke a generated method that has no arguments.
		(Similar to java.lang.refect.Method.invoke)

		Returns the value returned by the method.

		@exception 	StandardException	Standard Cloudscape error policy
	*/

	public Object invoke(Object ref)
		throws StandardException;
}
