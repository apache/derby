/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.reference
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.reference;

public interface Module {

	String CacheFactory = "org.apache.derby.iapi.services.cache.CacheFactory";
	String CipherFactory = "org.apache.derby.iapi.services.crypto.CipherFactory";
	String ClassFactory = "org.apache.derby.iapi.services.loader.ClassFactory";
	String DaemonFactory = "org.apache.derby.iapi.services.daemon.DaemonFactory";
	String JavaFactory ="org.apache.derby.iapi.services.compiler.JavaFactory";
	String LockFactory = "org.apache.derby.iapi.services.locks.LockFactory";
	String PropertyFactory = "org.apache.derby.iapi.services.property.PropertyFactory";
	String ResourceAdapter = "org.apache.derby.iapi.jdbc.ResourceAdapter";

}
