/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import java.lang.reflect.InvocationTargetException;

public interface InstanceGetter { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	/**
		Create an instance of a class.

		@exception InstantiationException Zero arg constructor can not be executed
		@exception IllegalAccessException Class or zero arg constructor is not public.
		@exception InvocationTargetException Exception throw in zero-arg constructor.

	*/
	public Object getNewInstance()
		throws InstantiationException, IllegalAccessException, InvocationTargetException;
}
