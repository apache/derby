/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import org.apache.derby.iapi.services.loader.ClassFactory;

import java.io.ObjectStreamClass;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
	An object input stream that implements resolve class in order
	to load the class through the ClassFactory.loadApplicationClass method.
*/
class ApplicationObjectInputStream extends ObjectInputStream
    implements ErrorObjectInput
{

	protected ClassFactory cf;
	protected ObjectStreamClass        initialClass;

	ApplicationObjectInputStream(InputStream in, ClassFactory cf)
		throws IOException {
		super(in);
		this.cf = cf;
	}

	protected Class resolveClass(ObjectStreamClass v)
		throws IOException, ClassNotFoundException {

		if (initialClass == null)
			initialClass = v;

		if (cf != null)
			return cf.loadApplicationClass(v);

		throw new ClassNotFoundException(v.getName());
	}

	public String getErrorInfo() {
		if (initialClass == null)
			return "";

		return initialClass.getName() + " (serialVersionUID="
			+ initialClass.getSerialVersionUID() + ")";
	}

	public Exception getNestedException() {
        return null;
	}

}
